The TD SFDC connector to export data currently doesn’t provide a way to log how many records were successfully exported, how many failed and so on. Also, when exporting a huge amount of data, the WF/job logs get truncated which means we cannot look at the logs to determine what exactly happened at the end of the process. But generally speaking, every customer using SFDC to export data will need to maintain a log to capture such stats.


## Prerequisites
Download td cli
Basic Knowledge of Treasure Workflow's syntax

## How to run this example
You can copy or clone this directory.

After copying the directory, you should update the yml file with correct info `config/params_sfdc.yml`

Then, push the workflow with td wf push command.
- `td wf push sfdc_get_export_stats`

Before running this workflow, we need to set secrets with `td wf secrets` command.
Example to set secrets:
- `td wf secrets --project sfdc_get_export_stats --set sfdc.client_id --set sfdc.client_secret --set sfdc.username --set sfdc.password --set td.apikey1 --set sfdc.security_token`


Then, let's start running the example workflow.
- `td wf start sfdc_get_export_stats sfdc_get_export_stats --session now`
