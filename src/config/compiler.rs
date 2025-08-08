use super::{builder::ConfigBuilder, Config, OutputId};
use indexmap::{IndexMap, IndexSet};

use super::{
    schema, ComponentKey, DataType, SinkOuter, SourceOuter, SourceOutput, TransformOuter,
    TransformOutput, WildcardMatching,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

#[derive(Debug, Clone)]
pub enum Node {
    Source {
        outputs: Vec<SourceOutput>,
    },
    Transform {
        in_ty: DataType,
        outputs: Vec<TransformOutput>,
    },
    Sink {
        ty: DataType,
    },
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Source { outputs } => {
                write!(f, "component_kind: source\n  outputs:")?;
                for output in outputs {
                    write!(f, "\n    {}", output)?;
                }
                Ok(())
            }
            Node::Transform { in_ty, outputs } => {
                write!(
                    f,
                    "component_kind: source\n  input_types: {in_ty}\n  outputs:"
                )?;
                for output in outputs {
                    write!(f, "\n    {}", output)?;
                }
                Ok(())
            }
            Node::Sink { ty } => {
                write!(f, "component_kind: sink\n  types: {ty}")
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Edge {
    from: OutputId,
    to: ComponentKey,
}

#[derive(Default)]
pub struct Graph {
    nodes: HashMap<ComponentKey, Node>,
    edges: Vec<Edge>,
}

impl Graph {
    pub fn new(
        sources: &IndexMap<ComponentKey, SourceOuter>,
        transforms: &IndexMap<ComponentKey, TransformOuter<String>>,
        sinks: &IndexMap<ComponentKey, SinkOuter<String>>,
        schema: schema::Options,
        wildcard_matching: WildcardMatching,
    ) -> Result<Self, Vec<String>> {
        Self::new_inner(sources, transforms, sinks, false, schema, wildcard_matching)
    }
    /* 调用 */
    fn new_inner(
        sources: &IndexMap<ComponentKey, SourceOuter>,
        transforms: &IndexMap<ComponentKey, TransformOuter<String>>,
        sinks: &IndexMap<ComponentKey, SinkOuter<String>>,
        ignore_errors: bool,
        schema: schema::Options,
        wildcard_matching: WildcardMatching,
    ) -> Result<Self, Vec<String>> {
        let mut graph = Graph::default();
        let mut errors = Vec::new();

        // First, insert all of the different node types
        for (id, config) in sources.iter() {
            graph.nodes.insert(
                id.clone(),
                Node::Source {
                    outputs: config.inner.outputs(schema.log_namespace()),
                },
            );
        }

        for (id, transform) in transforms.iter() {
            graph.nodes.insert(
                id.clone(),
                Node::Transform {
                    in_ty: transform.inner.input().data_type(),
                    outputs: transform.inner.outputs(
                        crate::enrichment_tables::enrichment::TableRegistry::default(),
                        &[(id.into(), schema::Definition::any())],
                        schema.log_namespace(),
                    ),
                },
            );
        }

        for (id, config) in sinks {
            graph.nodes.insert(
                id.clone(),
                Node::Sink {
                    ty: config.inner.input().data_type(),
                },
            );
        }

        // With all of the nodes added, go through inputs and add edges, resolving strings into
        // actual `OutputId`s along the way.
        let available_inputs = graph.input_map()?;

        for (id, config) in transforms.iter() {
            for input in config.inputs.iter() {
                if let Err(e) = graph.add_input(input, id, &available_inputs, wildcard_matching) {
                    errors.push(e);
                }
            }
        }

        for (id, config) in sinks {
            for input in config.inputs.iter() {
                if let Err(e) = graph.add_input(input, id, &available_inputs, wildcard_matching) {
                    errors.push(e);
                }
            }
        }

        if ignore_errors || errors.is_empty() {
            Ok(graph)
        } else {
            Err(errors)
        }
    }

    fn add_input(
        &mut self,
        from: &str,
        to: &ComponentKey,
        available_inputs: &HashMap<String, OutputId>,
        wildcard_matching: WildcardMatching,
    ) -> Result<(), String> {
        if let Some(output_id) = available_inputs.get(from) {
            self.edges.push(Edge {
                from: output_id.clone(),
                to: to.clone(),
            });
            Ok(())
        } else {
            let output_type = match self.nodes.get(to) {
                Some(Node::Transform { .. }) => "transform",
                Some(Node::Sink { .. }) => "sink",
                _ => panic!("only transforms and sinks have inputs"),
            };
            // allow empty result if relaxed wildcard matching is enabled
            match wildcard_matching {
                WildcardMatching::Relaxed => {
                    // using value != glob::Pattern::escape(value) to check if value is a glob
                    // TODO: replace with proper check when https://github.com/rust-lang/glob/issues/72 is resolved
                    if from != glob::Pattern::escape(from) {
                        info!("Input \"{from}\" for {output_type} \"{to}\" didn’t match any components, but this was ignored because `relaxed_wildcard_matching` is enabled.");
                        return Ok(());
                    }
                }
                WildcardMatching::Strict => {}
            }
            info!(
                "Available components:\n{}",
                self.nodes
                    .iter()
                    .map(|(key, node)| format!("\"{}\":\n  {}", key, node))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
            Err(format!(
                "Input \"{from}\" for {output_type} \"{to}\" doesn't match any components.",
            ))
        }
    }

    /// Return the input type of a given component.
    ///
    /// # Panics
    ///
    /// Will panic if the given key is not present in the graph or identifies a source, which can't
    /// have inputs.
    fn get_input_type(&self, key: &ComponentKey) -> DataType {
        match self.nodes[key] {
            Node::Source { .. } => panic!("no inputs on sources"),
            Node::Transform { in_ty, .. } => in_ty,
            Node::Sink { ty } => ty,
        }
    }

    /// Return the output type associated with a given `OutputId`.
    ///
    /// # Panics
    ///
    /// Will panic if the given id is not present in the graph or identifies a sink, which can't
    /// have inputs.
    fn get_output_type(&self, id: &OutputId) -> DataType {
        match &self.nodes[&id.component] {
            Node::Source { outputs } => outputs
                .iter()
                .find(|output| output.port == id.port)
                .map(|output| output.ty)
                .expect("output didn't exist"),
            Node::Transform { outputs, .. } => outputs
                .iter()
                .find(|output| output.port == id.port)
                .map(|output| output.ty)
                .expect("output didn't exist"),
            Node::Sink { .. } => panic!("no outputs on sinks"),
        }
    }

    pub fn typecheck(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // check that all edges connect components with compatible data types
        for edge in &self.edges {
            let from_ty = self.get_output_type(&edge.from);
            let to_ty = self.get_input_type(&edge.to);

            if !from_ty.intersects(to_ty) {
                errors.push(format!(
                    "Data type mismatch between {} ({}) and {} ({})",
                    edge.from, from_ty, edge.to, to_ty
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            errors.sort();
            errors.dedup();
            Err(errors)
        }
    }

    pub fn check_for_cycles(&self) -> Result<(), String> {
        // find all sinks
        let sinks = self.nodes.iter().filter_map(|(name, node)| match node {
            Node::Sink { .. } => Some(name),
            _ => None,
        });

        // run DFS from each sink while keep tracking the current stack to detect cycles
        for s in sinks {
            let mut traversal: VecDeque<ComponentKey> = VecDeque::new();
            let mut visited: HashSet<ComponentKey> = HashSet::new();
            let mut stack: IndexSet<ComponentKey> = IndexSet::new();

            traversal.push_back(s.to_owned());
            while !traversal.is_empty() {
                let n = traversal.back().expect("can't be empty").clone();
                if !visited.contains(&n) {
                    visited.insert(n.clone());
                    stack.insert(n.clone());
                } else {
                    // we came back to the node after exploring all its children - remove it from the stack and traversal
                    stack.shift_remove(&n);
                    traversal.pop_back();
                }
                let inputs = self
                    .edges
                    .iter()
                    .filter(|e| e.to == n)
                    .map(|e| e.from.clone());
                for input in inputs {
                    if !visited.contains(&input.component) {
                        traversal.push_back(input.component);
                    } else if stack.contains(&input.component) {
                        // we reached the node while it is on the current stack - it's a cycle
                        let path = stack
                            .iter()
                            .skip(1) // skip the sink
                            .rev()
                            .map(|item| item.to_string())
                            .collect::<Vec<_>>();
                        return Err(format!(
                            "Cyclic dependency detected in the chain [ {} -> {} ]",
                            input.component.id(),
                            path.join(" -> ")
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn valid_inputs(&self) -> HashSet<OutputId> {
        self.nodes
            .iter()
            .flat_map(|(key, node)| match node {
                Node::Sink { .. } => vec![],
                Node::Source { outputs } => outputs
                    .iter()
                    .map(|output| OutputId {
                        component: key.clone(),
                        port: output.port.clone(),
                    })
                    .collect(),
                Node::Transform { outputs, .. } => outputs
                    .iter()
                    .map(|output| OutputId {
                        component: key.clone(),
                        port: output.port.clone(),
                    })
                    .collect(),
            })
            .collect()
    }

    /// 产生输出的map
    pub fn input_map(&self) -> Result<HashMap<String, OutputId>, Vec<String>> {
        let mut mapped: HashMap<String, OutputId> = HashMap::new();
        let mut errors = HashSet::new();

        for id in self.valid_inputs() {
            if let Some(_other) = mapped.insert(id.to_string(), id.clone()) {
                errors.insert(format!("Input specifier {} is ambiguous", id));
            }
        }

        if errors.is_empty() {
            Ok(mapped)
        } else {
            Err(errors.into_iter().collect())
        }
    }

    pub fn inputs_for(&self, node: &ComponentKey) -> Vec<OutputId> {
        self.edges
            .iter()
            .filter(|edge| &edge.to == node)
            .map(|edge| edge.from.clone())
            .collect()
    }
}

/* Compile, builder包含了配置项 */
pub fn compile(builder: ConfigBuilder) -> Result<(Config, Vec<String>), Vec<String>> {
    let mut errors = Vec::new();
    let ConfigBuilder {
        global,
        schema,
        enrichment_tables,
        sources,
        sinks,
        transforms,
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

    let sources_and_table_sources = sources
        .clone()
        .into_iter()
        .chain(
            enrichment_tables
                .iter()
                .filter_map(|(key, table)| table.as_source(key)),
        )
        .collect::<IndexMap<_, _>>();
    info!(
        "sources_and_table_sources sinks: {:?}",
        sources_and_table_sources
    );
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
    /* 再建一个新的config返回 */
    if errors.is_empty() {
        let config = Config {
            global,
            schema,
            enrichment_tables,
            sources,
            sinks,
            transforms,
            graceful_shutdown_duration,
        };

        let warnings = vec![];

        Ok((config, warnings))
    } else {
        Err(errors)
    }
}