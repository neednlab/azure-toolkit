using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using EtlLib;

namespace AzureToolkit
{
    class AzureToolkit
    {
        // Azure URLs
        public static readonly string armHost = "https://management.chinacloudapi.cn/";
        public static readonly string azureLoginHost = "https://login.chinacloudapi.cn/";
        public static readonly string azureAdLoginHost = "https://login.partner.microsoftonline.cn";

        // Azure resource info
        public static readonly string subscriptionId = ConfigurationManager.AppSettings["subscriptionId"];
        public static readonly string tenantID = ConfigurationManager.AppSettings["tenantID"];
        public static readonly string adfResourceGroup = ConfigurationManager.AppSettings["adfResourceGroup"];
        public static readonly string applicationId = ConfigurationManager.AppSettings["applicationId"];
        public static readonly string authenticationKey = ConfigurationManager.AppSettings["authenticationKey"];

        // Job config
        public static readonly int concurrency = int.Parse(ConfigurationManager.AppSettings["concurrency"]);
        public static readonly int maxRetryTimes = int.Parse(ConfigurationManager.AppSettings["maxRetryTimes"]);
        public static readonly int maxDurationMinutes = int.Parse(ConfigurationManager.AppSettings["maxDurationMinutes"]);


        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("ERROR: Missing 3 arguments [executionMode/appMode/appArg]");
                return;
            }

            string executionMode = args[0];
            string appMode = args[1];
            string jobListFilePath = args[2];
            string mailSubject = args[2];
            string targetEnv = args[2];

            if (executionMode != "auto" && executionMode != "manual")
            {
                Console.WriteLine("ERROR: executionMode must be [auto/manual]");
                return;
            }

            if (appMode == "job")
            {
                EtlLib.EtlLib.JobLauncher
                (
                   EtlJob.EtlJobType.dataFactory,
                   jobListFilePath: jobListFilePath,
                   concurrency: concurrency,
                   maxRetryTimes: maxRetryTimes,
                   maxDuration: maxDurationMinutes
                );
            }
            else if (appMode == "clone_dataset")
            {
                AzureDataFactoryProcessor adf = new AzureDataFactoryProcessor();
                adf.CloneDatasets(adf.ListDatasetResources(), targetEnv);           
            }

            else if (appMode == "clone_pipeline")
            {
                AzureDataFactoryProcessor adf = new AzureDataFactoryProcessor();
                adf.ClonePipelines(adf.ListPipelineResources(), targetEnv, null ,false);
            }
            else if (appMode == "migrate_pipeline")
            {
                AzureDataFactoryProcessor adf = new AzureDataFactoryProcessor();
                adf.ClonePipelines(adf.ListPipelineResources(), targetEnv, null, true);
            }

            else if (appMode == "mail")
            {
                string dataFactoryName = ConfigurationManager.AppSettings["dataFactoryName"];
                string smtpHost = ConfigurationManager.AppSettings["smtpHost"];
                string smtpUser = ConfigurationManager.AppSettings["smtpUser"];

                // Password can be stored in Azure Key Vault
                // string smtpPassword = AzureKeyVaultProcessor.GetSecret("DwMailPassword");
                string smtpPassword = ConfigurationManager.AppSettings["smtpPassword"];
                string mailFrom = ConfigurationManager.AppSettings["mailFrom"];
                string mailTo = ConfigurationManager.AppSettings["mailTo"];
                string mailCc = ConfigurationManager.AppSettings["mailCc"];

                //Filter pipeline name to monitor
                List<string> pipelines = new List<string>();
                var rows = File.ReadLines("Resource/check_jobs.csv", Encoding.UTF8).Skip(1);

                foreach (string row in rows)
                {
                    pipelines.Add(row);
                } 

                AzureDataFactoryProcessor adf = new AzureDataFactoryProcessor();

                var mainPipelineLog = adf.GetMainPipelineLog(dataFactoryName, pipelines);
                var abnormalPipelineLog = adf.GetAbnormalPipelineLog(dataFactoryName, pipelines);
                adf.SendADFLogCheckMail
                (
                    mainPipelineLog, abnormalPipelineLog,
                    smtpHost: smtpHost,
                    smtpUser: smtpUser,
                    smtpPassword: smtpPassword,
                    mailFrom: mailFrom,
                    mailTo: mailTo,
                    mailCc: mailCc,
                    mailSubject: mailSubject
                );

            }
            else
            {
                Console.WriteLine("appMode must be job/mail/clone_dataset/clone_pipeline");
                return;
            }

            if (executionMode != "auto")
            {
                Console.WriteLine();
                Console.WriteLine("All tasks compeleted, please press ENTER to exit");
                Console.ReadLine();
            }
        }

        static void TestADF()
        {
            string dataFactoryName = ConfigurationManager.AppSettings["dataFactoryName"];
         
            // Call pipeline
            string pipelineName = "pos_pipeline";

            // Define parameters
            Dictionary<string, object> parameters = new Dictionary<string, object>
            {
                { "waitTime", 30 },
                { "p2", "Hello World" }
            };

            //AzureDataFactoryProcessor.RunPipeline(dataFactoryName, pipelineName, parameters);

        }

        
    }
}
