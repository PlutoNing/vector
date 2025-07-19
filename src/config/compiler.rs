use super::{
    builder::ConfigBuilder, graph::Graph, transform::get_transform_output_ids,
    Config,
    OutputId,
};

use indexmap::{IndexMap, IndexSet};
use vector_lib::id::Inputs;
/* 什么是compile, builder包含了配置项 */
pub fn compile(mut builder: ConfigBuilder) -> Result<(Config, Vec<String>), Vec<String>> {
    let mut errors = Vec::new();

    expand_globs(&mut builder);

    let ConfigBuilder {
        global,
        schema,
        healthchecks,
        enrichment_tables,
        sources,
        sinks,
        transforms,
        tests,
        provider: _,
        graceful_shutdown_duration,
        allow_empty: _,
    } = builder;
    let all_sinks = sinks
        .clone()
        .into_iter()
        .chain(
            enrichment_tables
                .iter()
                .filter_map(|(key, table)| table.as_sink(key)),
        )
        .collect::<IndexMap<_, _>>();
    info!("all_sinks sinks: {:?}", all_sinks);
    let sources_and_table_sources = sources
        .clone()
        .into_iter()
        .chain(
            enrichment_tables
                .iter()
                .filter_map(|(key, table)| table.as_source(key)),
        )
        .collect::<IndexMap<_, _>>();
            info!("sources_and_table_sources sinks: {:?}", sources_and_table_sources);
    let graph = match Graph::new(
        &sources_and_table_sources,
        &transforms,
        &all_sinks,
        schema,
        global.wildcard_matching.unwrap_or_default(),
    ) {
        Ok(graph) => graph,
        Err(graph_errors) => {
            errors.extend(graph_errors);
            return Err(errors);
        }
    };
//  info!("graph sinks: {:?}", graph);
    if let Err(type_errors) = graph.typecheck() {
        errors.extend(type_errors);
    }

    if let Err(e) = graph.check_for_cycles() {
        errors.push(e);
    }

    // Inputs are resolved from string into OutputIds as part of graph construction, so update them
    // here before adding to the final config (the types require this).
    let sinks = sinks
        .into_iter()
        .map(|(key, sink)| {
            let inputs = graph.inputs_for(&key);
            (key, sink.with_inputs(inputs))
        })
        .collect();
    let transforms = transforms
        .into_iter()
        .map(|(key, transform)| {
            let inputs = graph.inputs_for(&key);
            (key, transform.with_inputs(inputs))
        })
        .collect();
    let enrichment_tables = enrichment_tables
        .into_iter()
        .map(|(key, table)| {
            let inputs = graph.inputs_for(&key);
            (key, table.with_inputs(inputs))
        })
        .collect();
    let tests = tests
        .into_iter()
        .map(|test| test.resolve_outputs(&graph))
        .collect::<Result<Vec<_>, Vec<_>>>()?;
        /* 再建一个新的config返回 */
    if errors.is_empty() {
        let mut config = Config {
            global,
            schema,
            healthchecks,
            enrichment_tables,
            sources,
            sinks,
            transforms,
            tests,
            graceful_shutdown_duration,
        };

        config.propagate_acknowledgements()?;

        let warnings = vec![];

        Ok((config, warnings))
    } else {
        Err(errors)
    }
}

/// Expand globs in input lists
pub(crate) fn expand_globs(config: &mut ConfigBuilder) {
    let candidates = config
        .sources
        .iter()
        .flat_map(|(key, s)| {
            s.inner
                .outputs(config.schema.log_namespace())
                .into_iter()
                .map(|output| OutputId {
                    component: key.clone(),
                    port: output.port,
                })
        })
        .chain(config.transforms.iter().flat_map(|(key, t)| {
            get_transform_output_ids(t.inner.as_ref(), key.clone(), config.schema.log_namespace())
        }))
        .map(|output_id| output_id.to_string())
        .collect::<IndexSet<String>>();

    for (id, transform) in config.transforms.iter_mut() {
        expand_globs_inner(&mut transform.inputs, &id.to_string(), &candidates);
    }

    for (id, sink) in config.sinks.iter_mut() {
        expand_globs_inner(&mut sink.inputs, &id.to_string(), &candidates);
    }
}

enum InputMatcher {
    Pattern(glob::Pattern),
    String(String),
}

impl InputMatcher {
    fn matches(&self, candidate: &str) -> bool {
        use InputMatcher::*;

        match self {
            Pattern(pattern) => pattern.matches(candidate),
            String(s) => s == candidate,
        }
    }
}

fn expand_globs_inner(inputs: &mut Inputs<String>, id: &str, candidates: &IndexSet<String>) {
    let raw_inputs = std::mem::take(inputs);
    for raw_input in raw_inputs {
        let matcher = glob::Pattern::new(&raw_input)
            .map(InputMatcher::Pattern)
            .unwrap_or_else(|error| {
                warn!(message = "Invalid glob pattern for input.", component_id = %id, %error);
                InputMatcher::String(raw_input.to_string())
            });
        let mut matched = false;
        for input in candidates {
            if matcher.matches(input) && input != id {
                matched = true;
                inputs.extend(Some(input.to_string()))
            }
        }
        // If it didn't work as a glob pattern, leave it in the inputs as-is. This lets us give
        // more accurate error messages about nonexistent inputs.
        if !matched {
            inputs.extend(Some(raw_input))
        }
    }
}