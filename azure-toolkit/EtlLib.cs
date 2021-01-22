using AzureToolkit;
using Microsoft.Azure.Management.DataFactory;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;


namespace EtlLib
{
    static class EtlLib
    {
        private enum MoveListMode { exection, success, failure };

        public enum InfoType { DEBUG, INFO, WARN, ERROR };

        public static void Print(InfoType info, string msg)
        {
            if (info != InfoType.DEBUG || true)
            {
                Console.WriteLine($"{info}: {msg}");
            }
        }

        public static void JobLauncher(EtlJob.EtlJobType jobType, Stream jobListFileStream = null, string jobListFilePath = "Resource/job.csv", int maxRetryTimes = 1, int maxDuration = 200, int concurrency = 2)
        {
            // Timer start here
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            // Define Wait list = W
            DataTable waitList = new DataTable("waitList");

            DataColumn[] waitListHeader =
            {
                new DataColumn("JOB",typeof(string)),
                new DataColumn("PARENT_JOB",typeof(string)) ,
                new DataColumn("FAILED_RUN",typeof(int)) ,
                new DataColumn("IS_TIME_SCHEDULED",typeof(int)) ,
                new DataColumn("START_HOUR",typeof(int)) ,
                new DataColumn("START_MINUTE",typeof(int))
                //new DataColumn("IS_DEAD",typeof(int))
            };
            waitList.Columns.AddRange(waitListHeader);

            // Define Execution list = E
            DataTable executionList = new DataTable("execList");

            DataColumn[] executionListHeader =
            {
                new DataColumn("JOB",typeof(string)),
                new DataColumn("RUN_ID",typeof(string)) ,
                new DataColumn("RUN_STATUS",typeof(string)) ,
                new DataColumn("RETRY_TIMES",typeof(int))
            };
            executionList.Columns.AddRange(executionListHeader);

            // Define Complete list = C
            // Cloned from E
            DataTable completeList = executionList.Clone();
            completeList.TableName = "completeList";

            // Read Wait List
            if (jobListFilePath != null)
            {

                var rows = File.ReadLines(jobListFilePath, Encoding.UTF8).Skip(1);

                foreach (string row in rows)
                {

                    string[] columns = row.Split(',');
                    DataRow waitDr = waitList.NewRow();

                    for (int i = 0; i < 6; i++)
                    {
                        if (!String.IsNullOrEmpty(columns[i]))
                        {
                            waitDr[i] = columns[i];
                        }

                    }
                    waitList.Rows.Add(waitDr);

                }
            }
            else
            {
                StreamReader sr = new StreamReader(jobListFileStream);
                // Header
                sr.ReadLine();
                while (sr.Peek() != -1)
                {
                    string row = sr.ReadLine();
                    string[] columns = row.Split(',');
                    DataRow waitDr = waitList.NewRow();

                    for (int i = 0; i < 6; i++)
                    {
                        if (!string.IsNullOrEmpty(columns[i]))
                        {
                            waitDr[i] = columns[i];
                        }

                    }
                    
                    waitList.Rows.Add(waitDr);
                }
            }

            int totalJobNumbers = waitList.Rows.Count;

            // Add validation here
            // Validate Q if exists in P
            // Validate if P->Q, Q->P
            Print(InfoType.INFO, $"Read CSV {totalJobNumbers} rows");

            // New adf object
            int tokenCounter = 1; 
            AzureDataFactoryProcessor adf = new AzureDataFactoryProcessor();

            // Loop condition
            // 1. Exists ANY alive P in W
            // 2. Exists ANY P in E
            while (waitList.Rows.Count > 0 || executionList.Rows.Count > 0)
            {
                // Quit if TIME UP
                if (stopWatch.ElapsedMilliseconds / 1000 / 60 >= maxDuration)
                {
                    Print(InfoType.ERROR ,$"Job Launcher TIME UP. Max duration is {maxDuration}m");
                    return;
                }

                // new adf every 60 mins         
                if ((stopWatch.ElapsedMilliseconds / 1000 / 60) > 60 * tokenCounter)
                {
                    adf = new AzureDataFactoryProcessor();
                    tokenCounter += 1;
                }
                

                // Query W which can move into E
                // 1.Time scheduled job
                int nowHourMinute = DateTime.Now.Hour * 100 + DateTime.Now.Minute;
                var startTimeJobQuery =
                    from p in waitList.AsEnumerable()
                    where (int)p["IS_TIME_SCHEDULED"] == 1 &&               
                        nowHourMinute >= (string.IsNullOrEmpty(p["START_HOUR"].ToString()) ? 0 : (int)p["START_HOUR"]) * 100 +
                                         (string.IsNullOrEmpty(p["START_MINUTE"].ToString()) ? 0 : (int)p["START_MINUTE"])
                    select p["JOB"];

                // 2.Dependent job
                var allDependentJobQuery =
                    from p in waitList.AsEnumerable()
                    where (int)p["IS_TIME_SCHEDULED"] == 0
                    select p["JOB"];

                var completeJobQuery =
                    from p in completeList.AsEnumerable()
                    select p["JOB"];

                var waitJobQuery =
                    from p in waitList.AsEnumerable()
                    where (!completeJobQuery.Contains(p["PARENT_JOB"])) && (int)p["IS_TIME_SCHEDULED"] == 0
                    select p["JOB"];

                // Find out top (N) of P, P in E can not > [concurrency]
                int topN = concurrency - executionList.Rows.Count;

                // The same as SQL Logic
                // SELECT TOP(concurrency) JOB FROM a EXCEPT
                // SELECT JOB FROM a 
                // WHERE a.PARENT_JOB NOT IN (SELECT JOB FROM c)
                var allStartJobQuery = startTimeJobQuery.Union(allDependentJobQuery.Except(waitJobQuery)).Take(topN);

                // Execute query here
                var allStartJobResult = allStartJobQuery.ToList();
                foreach (string p in allStartJobResult)
                {
                    MoveJobList(p, waitList, executionList, MoveListMode.exection);
                }

                // Launch all job in E (wait & failed which to retry)
                DataRow[] executionListRows = executionList.Select("RUN_STATUS IN ('Wait','Failed')");
                foreach (DataRow execJob in executionListRows)
                {
                    string job = execJob["JOB"].ToString();
                    string runId = "";
                    int execJobRetryTimes = (int)execJob["RETRY_TIMES"];

                    try
                    {
                        runId = EtlJob.Launch(adf, jobType, job);
                    }
                    catch (Exception ex)
                    {
                        // If lanuch failed times >= maxRetryTimes, move P to C
                        if (execJobRetryTimes >= maxRetryTimes)
                        {
                            FailedAction(true, execJob, executionList, completeList, waitList);
                        }
                        execJob["RETRY_TIMES"] = execJobRetryTimes + 1;

                        Print(InfoType.ERROR, ex.Message + " " + ex.InnerException.Message);
                        Print(InfoType.ERROR, $"Launch job {job} failed");
                    }

                    // Set(Reset) RUN_ID & RETRY_TIMES
                    execJob["RUN_ID"] = runId;
                    if (execJob["RUN_STATUS"].ToString() == "Failed")
                    {
                        execJob["RETRY_TIMES"] = execJobRetryTimes + 1;
                    }
                }

                // Check job Status
                // Copy executionList to a DataRow[], using in foreach
                executionListRows = executionList.Select();
                foreach (DataRow execJob in executionListRows)
                {
                    string runId = execJob["RUN_ID"].ToString();
                    // If RUN_ID is empty, quit and go to next
                    if (string.IsNullOrEmpty(runId))
                    {
                        continue;
                    }

                    string execJobName = execJob["JOB"].ToString();
                    int execJobRetryTimes = (int)execJob["RETRY_TIMES"];

                    // Start check job status
                    string status = EtlJob.Check(adf, jobType, runId);
                    execJob["RUN_STATUS"] = status;

                    // Failed or Cancelled, move P to C
                    if ((status == "Failed" && execJobRetryTimes >= maxRetryTimes) || status == "Cancelled") 
                    {
                        FailedAction(false, execJob, executionList, completeList, waitList);
                    }         
                    // Succeeded, move P to C
                    else if (status == "Succeeded")
                    {
                        MoveJobList
                        (
                            job: execJobName,
                            sourceList: executionList,
                            targetList: completeList,
                            mode: MoveListMode.success,
                            runId: runId,
                            runStatus: status
                        );
                    }
                    // Queued, InProgress
                    else
                    {
                        Thread.Sleep(2000);
                    }

                    string retriedPrint = "";
                    if (execJobRetryTimes > 0)
                    {
                        retriedPrint = $" (retried {execJobRetryTimes})";
                    }

                    InfoType info = (status == "Failed" ? InfoType.ERROR : InfoType.INFO);

                    Print(info, $"{execJobName}{retriedPrint} {status} at {DateTime.Now}");           

                }
            }
            Print(InfoType.INFO, $"Job Launcher completed. Duration {stopWatch.ElapsedMilliseconds / 1000}s");
        }

        private static void MoveJobList(string job, DataTable sourceList, DataTable targetList, MoveListMode mode, string runId = null, string runStatus = null, int jobRetryTimes = 0)
        {
            DataRow targetRow = targetList.NewRow();
            targetRow["JOB"] = job;
            if (mode == MoveListMode.exection)
            {
                targetRow["RUN_STATUS"] = "Wait";

            }
            if (mode == MoveListMode.success | mode == MoveListMode.failure)
            {
                targetRow["RUN_ID"] = runId;
                targetRow["RUN_STATUS"] = runStatus;
            }
            targetRow["RETRY_TIMES"] = jobRetryTimes;
            targetList.Rows.Add(targetRow);


            // Remove job in source list
            // SQL: DELETE FROM table WHERE JOB = ?
            DataRow[] deleteJobs = sourceList.Select($"JOB = '{job}'");
            foreach (DataRow p in deleteJobs)
            {
                sourceList.Rows.Remove(p);
               
            }
            Print(InfoType.DEBUG, $"Move job {job} into {targetList.TableName}");
        }

        private static void FailedAction(bool isLaunchFailed, DataRow execJob, DataTable executionList, DataTable completeList, DataTable waitList)
        {

            string execJobName = execJob["JOB"].ToString();
            string runId = execJob["RUN_ID"].ToString();
            string status = isLaunchFailed ? "Launch failed" : "Failed";
            int execJobRetryTimes = (int)execJob["RETRY_TIMES"];

            MoveJobList
            (
                job: execJobName,
                sourceList: executionList,
                targetList: completeList,
                mode: MoveListMode.failure,
                runId: runId,
                runStatus: status,
                jobRetryTimes: execJobRetryTimes

            );

            // Set P to DEAD in W [recursively]
            // Move P to C
            List<string> deadJobs = new List<string>();

            IEnumerable<object> jobQuery = new string[] { execJobName };
            SetJobDead(jobQuery, waitList, deadJobs);

            foreach(string deadJob in deadJobs)
            {
                MoveJobList
                (
                    job: deadJob,
                    sourceList: waitList,
                    targetList: completeList,
                    mode: MoveListMode.failure,
                    runId: "None",
                    runStatus: "Dead",
                    jobRetryTimes: 0

                );
                Print(InfoType.WARN, $"Job { deadJob} is DEAD");
            }

        }

        private static void SetJobDead(IEnumerable<object> query, DataTable waitList, List<string> deadJobs)
        {
            if (query.Count() == 0)
            {
                return;
            }

            foreach (string jobName in query.Distinct())
            {
                var newDeadJobQuery =
                    from job in waitList.Select($"PARENT_JOB = '{jobName}'  AND FAILED_RUN = 0").AsEnumerable()
                    select job["JOB"];

                foreach (string parentJobName in newDeadJobQuery.Distinct())
                {
                    deadJobs.Add(parentJobName);
                }

                // Recursion 
                SetJobDead(newDeadJobQuery, waitList, deadJobs);
            }
        }

    }

    class EtlJob
    {
        public enum EtlJobType { dataFactory, SSIS };
        public static string Launch(AzureDataFactoryProcessor adf, EtlJobType jobType, string job)
        {
            string runId = "";

            if (jobType == EtlJobType.dataFactory)
            {
                runId = adf.RunPipeline(job);

            }

            return runId;
        }

        public static string Check(AzureDataFactoryProcessor adf, EtlJobType jobType, string runId)
        {
            string status = "";

            if (jobType == EtlJobType.dataFactory)
            {
                status = adf.CheckPipeline(runId);
            }

            return status;
        }
    }
}