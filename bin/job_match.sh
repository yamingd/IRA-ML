#!/bin/sh
hadoop jar ~/ira/ira-1.0-SNAPSHOT-job.jar com.ml.ira.match.ActionTablePrepareJob
hadoop jar ~/ira/ira-1.0-SNAPSHOT-job.jar com.ml.ira.match.MatchMatrixBuildJob
hadoop jar ~/ira/ira-1.0-SNAPSHOT-job.jar com.ml.ira.match.DForestBuildJob