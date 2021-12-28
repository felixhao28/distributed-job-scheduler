#! /bin/bash
# This trap command is optional but I want to trap kill signals so the subprocesses will terminate when I kill this shell script.
# Otherwise, when this shell script is killed by user, the scheduler will treat it as a completion but the subprocess might still be running.
# Note: this is optional for the scheduler to work. It is just my personal preference.
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
# Exits the script upon first failed command.
# Note: this is also optional and might even work against your interest when failing is expected.
set -e

# The JOB_ID environment variable stores the id of the job. All output from this script will be logged into <log_dir>/<JOB_ID>.txt
# JOB_ID is guaranteed to be unique and ascending in regards to the start time of the job.
# All arguments used in "add_job" will be passed to this script.
echo "Starting job input_file=$1 as $JOB_ID"
INPUT_FILE=$1

REMOTE_DIR=/data/jobs
echo "Copying $INPUT_FILE to $SLAVE_IP:$REMOTE_DIR"
# "-t -t" flags allow us to kill the remote process when ssh is killed, which is essential to job cancellation.
ssh -t -t $SLAVE_IP mkdir -p $REMOTE_DIR
rsync -aP $INPUT_FILE $SLAVE_IP:$REMOTE_DIR
echo "File copied"

# Example job, counting the words in the file
echo "Counting words"
ssh -t -t $SLAVE_IP "wc $REMOTE_DIR/$INPUT_FILE > $REMOTE_DIR/$JOB_ID.out"
echo "Job done"

# Create a directory to store the results
echo "Copying $REMOTE_DIR/$JOB_ID.out to finished_work/"
mkdir -p finished_work
rsync -aP $SLAVE_IP:$REMOTE_DIR/$JOB_ID.out finished_work
# Rename it for better file name
mv finished_work/$JOB_ID.out finished_work/${JOB_ID}_$(basename $1).out

echo "Bye"
