
== Slurm Backend ==

The Slurm backend uses the sacct command from slurm to gather information from
Slurm on jobs.

== Config options ==

In the [slurm] section in the bart.conf file the following options are
available.

statefile: default=slurm.state
Name of the state file where the Slurm backend saves how many jobs that are sent
to SGAS.

statefile_default: default=50000
Number of seconds to go back if state file is missing.

idtimestamp: default=true
Adds a timestamp to the recordid to be sure that recordid is unique if Slurm
reuses the jobid.

max_days: default=7
Max number of days to process for every run of bart.

billing_unit: default=cpu
Which element of the AllocTRES should be used as the "number of cpus" in the
reported job records.  If one sets up TRESBillingWeights in Slurm, using
'billing_unit=billing' makes SGAS-BART report the same job "size" as Slurm
uses for accounting.
