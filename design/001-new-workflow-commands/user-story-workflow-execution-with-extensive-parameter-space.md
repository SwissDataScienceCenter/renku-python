## RenkuLab use case for workflow execution while exploring parameter space: 
Reference repo: [mnn_omni_batch](https://renkulab.io/projects/omnibenchmark/omni_batch/mnn-omni-batch)

### Context

Our framework consists of multiple repositories connected and coordinated using knowledge graph queries. 
Each method workflow repository runs a method as part of a benchmark with a set of predefined parameter combinations 
on every available data file (annotated with a spec ific keyword and in the right format). 
Resulting files produced by all parameter combinations are added to another renku dataset that groups all outputs of a given method. 

Overall, there are 3 datasets associated with such method workflow project:

+ Input files: [omni_batch_processed]( https://renkulab.io/projects/omnibenchmark/omni_data/omni-batch-processed ) a bundle of all data files to run the method on. 
+ Parameter space: [omni_batch_param](https://renkulab.io/projects/omnibenchmark/omni_batch/omni-batch-param ) contains a .yaml file with parameter names and values.
+ Output files: [mnn_omni_batch](https://renkulab.io/projects/omnibenchmark/omni_batch/mnn-omni-batch) bundles all results (method outputs).

In `src/run_mnn.sh` these datasets are imported or updated and all possible parameter combinations are generated. 
The file `src/workflows/define_workflow.sh` describes a workflow to run the method by calling `src/mnn.R` and track it with renku run. 
Utils functions are loaded from the submodule [omnibenchmark/utils](https://renkulab.io/projects/omnibenchmark/utils).

### Problem

We are looking for the most efficient way to run and maintain benchmarking methods with all possible data files and parameter combinations by 
describing them as workflows with outputs, that can be automatically updated via `renku update` or `renku dataset update`. 
Given that the new data will be continuously added, the script should generate new workflows for every method whenever the new data files appear. 

One way is to generate a new workflow per data file and parameter combination. For this we would query the KG for all existing workflows, 
compare them to all data file-parameter combinations and generate new workflows for new combinations by running `src/workflows/define_workflow.sh`, 
while updating the existing ones using `renku update`. 
For every method, benchmarking requires testing different parameter combinations for each available data file. 
As little as 10 data files processed with 4 parameters with 3 values each would already result in over a 100 workflows, 
posing the challenges for method maintenance and interpretation of benchmarking results. 
A substantial effort will be needed to handle such a number of workflows in case of any changes/edits or error messages. 
Furthermore, generating a workflow for every method-data-parameter combination is redundant and will rapidly increase in complexity as the number of 
methods and data increases.
Given that the planned workflow commands will allow us to modify parameters for workflow template execution, 
we aspire to rely on the new functionality `renku workflow loop` to search the whole parameter space, 
rather than developing complex and redundant solution described above. 
We hope that implementation and maybe also handling of workflows e.g., in case we want to edit, replace or remove the 
workflows would be simplified by using this new feature.

### Desired solution

Generate a single method workflow template per data file with an initial set of default values. 
Execute workflow template multiple times with all parameter combinations specified in `src/mnn.R` producing different output files for 
all of those combinations in `data/mnn_omni_batch`. 
Upon the update of source dataset `omni_batch_param`, use a single command to update all workflow template executions (e.g. `renku update`). 
When the new features become available, such as `renku workflow edit` apply them on workflow templates (in this case one for each dataset) 
instead of numerous dataset-parameter combinations. 
While we will try to filter in advance it is still to expect that some parameter combinations will fail. 
An optimal solution would enable us to detect and exclude those from the parameter space and further updates.
```
command=(R CMD BATCH --no-restore --no-save "$R_args_parsed" "$R_params_parsed"
         src/process_data.R log/process_${dataset_name}.Rout)

renku workflow loop --name $dataset_name $inputs_parsed --mapping $workflow_yaml" 
```

