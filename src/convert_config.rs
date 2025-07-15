use crate::config::{format, ConfigBuilder, Format};
use clap::Parser;
use colored::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(rename_all = "kebab-case")]
pub struct Opts {
    /// The input path. It can be a single file or a directory. If this points to a directory,
    /// all files with a "toml", "yaml" or "json" extension will be converted.
    pub(crate) input_path: PathBuf,

    /// The output file or directory to be created. This command will fail if the output directory exists.
    pub(crate) output_path: PathBuf,

    /// The target format to which existing config files will be converted to.
    #[arg(long, default_value = "yaml")]
    pub(crate) output_format: Format,
}

fn check_paths(opts: &Opts) -> Result<(), String> {
    let in_metadata = fs::metadata(&opts.input_path)
        .unwrap_or_else(|_| panic!("Failed to get metadata for: {:?}", &opts.input_path));

    if opts.output_path.exists() {
        return Err(format!(
            "Output path {:?} already exists. Please provide a non-existing output path.",
            opts.output_path
        ));
    }

    if opts.output_path.extension().is_none() {
        if in_metadata.is_file() {
            return Err(format!(
                "{:?} points to a file but {:?} points to a directory.",
                opts.input_path, opts.output_path
            ));
        }
    } else if in_metadata.is_dir() {
        return Err(format!(
            "{:?} points to a directory but {:?} points to a file.",
            opts.input_path, opts.output_path
        ));
    }

    Ok(())
}

pub(crate) fn cmd(opts: &Opts) -> exitcode::ExitCode {
    if let Err(e) = check_paths(opts) {
        #[allow(clippy::print_stderr)]
        {
            eprintln!("{}", e.red());
        }
        return exitcode::SOFTWARE;
    }

    if opts.input_path.is_file() && opts.output_path.extension().is_some() {
        if let Some(base_dir) = opts.output_path.parent() {
            if !base_dir.exists() {
                fs::create_dir_all(base_dir).unwrap_or_else(|_| {
                    panic!("Failed to create output dir(s): {:?}", &opts.output_path)
                });
            }
        }

        match convert_config(&opts.input_path, &opts.output_path, opts.output_format) {
            Ok(_) => exitcode::OK,
            Err(errors) => {
                #[allow(clippy::print_stderr)]
                {
                    errors.iter().for_each(|e| eprintln!("{}", e.red()));
                }
                exitcode::SOFTWARE
            }
        }
    } else {
        match walk_dir_and_convert(&opts.input_path, &opts.output_path, opts.output_format) {
            Ok(()) => {
                #[allow(clippy::print_stdout)]
                {
                    println!(
                        "Finished conversion(s). Results are in {:?}",
                        opts.output_path
                    );
                }
                exitcode::OK
            }
            Err(errors) => {
                #[allow(clippy::print_stderr)]
                {
                    errors.iter().for_each(|e| eprintln!("{}", e.red()));
                }
                exitcode::SOFTWARE
            }
        }
    }
}

fn convert_config(
    input_path: &Path,
    output_path: &Path,
    output_format: Format,
) -> Result<(), Vec<String>> {
    if output_path.exists() {
        return Err(vec![format!("Output path {output_path:?} exists")]);
    }
    let input_format = match Format::from_str(
        input_path
            .extension()
            .unwrap_or_else(|| panic!("Failed to get extension for: {input_path:?}"))
            .to_str()
            .unwrap_or_else(|| panic!("Failed to convert OsStr to &str for: {input_path:?}")),
    ) {
        Ok(format) => format,
        Err(_) => return Ok(()), // skip irrelevant files
    };

    if input_format == output_format {
        return Ok(());
    }

    #[allow(clippy::print_stdout)]
    {
        println!("Converting {input_path:?} config to {output_format:?}.");
    }
    let file_contents = fs::read_to_string(input_path).map_err(|e| vec![e.to_string()])?;
    let builder: ConfigBuilder = format::deserialize(&file_contents, input_format)?;
    let config = builder.build()?;
    let output_string =
        format::serialize(&config, output_format).map_err(|e| vec![e.to_string()])?;
    fs::write(output_path, output_string).map_err(|e| vec![e.to_string()])?;

    #[allow(clippy::print_stdout)]
    {
        println!("Wrote result to {output_path:?}.");
    }
    Ok(())
}

fn walk_dir_and_convert(
    input_path: &Path,
    output_dir: &Path,
    output_format: Format,
) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    if input_path.is_dir() {
        for entry in fs::read_dir(input_path)
            .unwrap_or_else(|_| panic!("Failed to read dir: {input_path:?}"))
        {
            let entry_path = entry
                .unwrap_or_else(|_| panic!("Failed to get entry for dir: {input_path:?}"))
                .path();
            let new_output_dir = if entry_path.is_dir() {
                let last_component = entry_path
                    .file_name()
                    .unwrap_or_else(|| panic!("Failed to get file_name for {entry_path:?}"));
                let new_dir = output_dir.join(last_component);

                if !new_dir.exists() {
                    fs::create_dir_all(&new_dir)
                        .unwrap_or_else(|_| panic!("Failed to create output dir: {new_dir:?}"));
                }
                new_dir
            } else {
                output_dir.to_path_buf()
            };

            if let Err(new_errors) = walk_dir_and_convert(
                &input_path.join(&entry_path),
                &new_output_dir,
                output_format,
            ) {
                errors.extend(new_errors);
            }
        }
    } else {
        let output_path = output_dir.join(
            input_path
                .with_extension(output_format.to_string().as_str())
                .file_name()
                .ok_or_else(|| {
                    vec![format!(
                        "Cannot create output path for input: {input_path:?}"
                    )]
                })?,
        );
        if let Err(new_errors) = convert_config(input_path, &output_path, output_format) {
            errors.extend(new_errors);
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}