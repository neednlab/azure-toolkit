using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using System.Configuration;
using System;
using System.Collections.Generic;
using System.Net.Mail;
using System.Text;
using static EtlLib.EtlLib;
using System.Net;
using System.Security.Cryptography.Xml;
using System.IO;

namespace AzureToolkit
{
    class AzureDataFactoryProcessor
    {
        // Read configuration
        static readonly string defaultDataFactoryName = ConfigurationManager.AppSettings["dataFactoryName"];
        readonly AuthenticationContext context; 
        readonly ClientCredential cc; 
        AuthenticationResult result;
        ServiceClientCredentials cred;

        // Authenticate and create a data factory management client
        public DataFactoryManagementClient client;

        public AzureDataFactoryProcessor()
        {
            cc = new ClientCredential(AzureToolkit.applicationId, AzureToolkit.authenticationKey);
            context = new AuthenticationContext(AzureToolkit.azureLoginHost + AzureToolkit.tenantID);
            result = context.AcquireTokenAsync(AzureToolkit.armHost, cc).Result; ;
            context.ExtendedLifeTimeEnabled = true;
            // Print(InfoType.DEBUG, result.AccessToken);

            cred = new TokenCredentials(result.AccessToken);
            client = new DataFactoryManagementClient(cred)
            {
                SubscriptionId = AzureToolkit.subscriptionId,
                BaseUri = new Uri(AzureToolkit.armHost)
            };

            Print(InfoType.DEBUG, "new AzureDataFactoryProcessor");


        }

        public string RunPipeline(string pipelineName, string dataFactoryName = null, Dictionary<string, object> parameters = null)
        {
            if (dataFactoryName == null)
            {
                dataFactoryName = defaultDataFactoryName;
            }
            CreateRunResponse runResponse = client.Pipelines.CreateRunWithHttpMessagesAsync
            (
                AzureToolkit.adfResourceGroup, dataFactoryName, pipelineName, parameters: parameters
            ).Result.Body;
            Print(InfoType.INFO, $"Pipeline {pipelineName} started, RUN ID: {runResponse.RunId}");
            return runResponse.RunId;
        }

        public string CheckPipeline(string runId, string dataFactoryName = null)
        {
            
            dataFactoryName = dataFactoryName ?? defaultDataFactoryName;
            try
            {
                PipelineRun res = client.PipelineRuns.Get(AzureToolkit.adfResourceGroup, dataFactoryName, runId);

                return res.Status;
            }
            catch (Exception ex)
            {
                Print(InfoType.ERROR, ex.Message);
                return "Unknown";
            }
        }

        public List<PipelineResource> ListPipelineResources(string dataFactoryName = null)
        {
            dataFactoryName = dataFactoryName ?? defaultDataFactoryName;
            var pg = client.Pipelines.ListByFactory(AzureToolkit.adfResourceGroup, dataFactoryName);

            List<PipelineResource> prs = new List<PipelineResource>();

            foreach (PipelineResource pipelineResource in pg)
            {
                prs.Add(pipelineResource);
            }

            // Read all pages      
            while (pg.NextPageLink != null)
            {
                pg = client.Pipelines.ListByFactoryNext(pg.NextPageLink);
                foreach (var pipelineResource in pg)
                {
                    prs.Add(pipelineResource);
                }
            }
            
            return prs;
                
        }

        private PipelineResource GetPipelineResource(string pipelineName, string dataFactoryName = null)
        {
            dataFactoryName = dataFactoryName ?? defaultDataFactoryName;
     
            PipelineResource pr = client.Pipelines.Get(AzureToolkit.adfResourceGroup, dataFactoryName, pipelineName);
            return pr;  
        }


        public void ClonePipeline(string pipelineName, string targetEnv = "dev", string dataFactoryName = null, bool isChangeContent = false)
        {
            string sourceEnv = "dev";

            // Check if pipeline name start with source env
            if (!pipelineName.StartsWith(sourceEnv + "_"))
            {
                Print(InfoType.WARN, $"Pipeline {pipelineName} does not start with [{sourceEnv}_], skipped clone!");
                return;
            }

            dataFactoryName = dataFactoryName ?? defaultDataFactoryName;

            // Get pipeline info
            try
            {
                 
                PipelineResource pr = GetPipelineResource(pipelineName, dataFactoryName);

                // Change pipeline folder
                string sourceFolder = pr.Folder.Name;
                string targetFolder = sourceFolder.Replace("dw_" + sourceEnv, "dw_" + targetEnv);
                pr.Folder = new PipelineFolder(targetFolder);

                if (isChangeContent)
                {
                    // Process STG pipeline
                    if (sourceFolder == "dw_dev/stg")
                    {
                        // stg pipeline only has one COPY DATA activity
                        CopyActivity copy = (CopyActivity)pr.Activities[0];
                        DatasetReference dr;

                        // Change source dataset
                        dr = copy.Inputs[0];
                        string targetDataSetName = dr.ReferenceName.Replace(sourceEnv + "_", targetEnv + "_");
                        dr.ReferenceName = targetDataSetName;

                        copy.Inputs.Clear();
                        copy.Inputs.Add(dr);

                        // Change sink dataset
                        dr = copy.Outputs[0];
                        targetDataSetName = dr.ReferenceName.Replace(sourceEnv + "_", targetEnv + "_");
                        dr.ReferenceName = targetDataSetName;

                        copy.Outputs.Clear();
                        copy.Outputs.Add(dr);

                        // Rest copy activity
                        pr.Activities.Clear();
                        pr.Activities.Add(copy);


                        // Change parameter sink_container default value
                        string sinkDbName = pr.Parameters["sink_container"].DefaultValue.ToString();
                        if (string.IsNullOrEmpty(sinkDbName))
                        {
                            Print(InfoType.WARN, $"Pipeline {pipelineName} does not have parameter [sink_container]");
                        }
                        else
                        {
                            string dbPrefix = targetEnv == "dev" ? "" : targetEnv + "-";
                            pr.Parameters["sink_container"].DefaultValue = dbPrefix + sinkDbName;
                        }
                    }
                    else if (sourceFolder == "dw_dev/edw" || sourceFolder == "dw_dev/dm")
                    {
                        // Change HDI Cluster
                        HDInsightSparkActivity spark = (HDInsightSparkActivity)pr.Activities[0];
                        string sourceHdiName = spark.LinkedServiceName.ReferenceName;
                        string targetHdiName = sourceHdiName.Replace("dw_dev", "dw_" + targetEnv);

                        LinkedServiceReference hdi = new LinkedServiceReference(targetHdiName);
                        spark.LinkedServiceName = hdi;

                        // Change script blob
                        string sourceScriptBlobName = spark.SparkJobLinkedService.ReferenceName;
                        string targetScriptBlobName = sourceScriptBlobName.Replace("dw_dev", "dw_" + targetEnv);
                        LinkedServiceReference scriptBlob = new LinkedServiceReference(targetScriptBlobName);
                        spark.SparkJobLinkedService = scriptBlob;

                        // Change script path
                        spark.RootPath = "uat-" + spark.RootPath;

                        pr.Activities.Clear();
                        pr.Activities.Add(spark);
                    }
                }
                // Change target pipeline name
                string clonePipelineName = targetEnv + "_" + pipelineName.Substring(4, pipelineName.Length - 4);
                
                // Create (or udpate) new pipeline 
                client.Pipelines.CreateOrUpdate(AzureToolkit.adfResourceGroup, dataFactoryName, clonePipelineName, pr);
                
                Print(InfoType.INFO, $"Clone pipeline {pipelineName} to {clonePipelineName}");
            }
            catch (Exception ex)
            {
                Print(InfoType.ERROR, $"Create or Update pipeline [{pipelineName}] failed");
                Print(InfoType.ERROR, ex.Message);
                return;
            }

        }

        public void ClonePipelines(List<PipelineResource> prs, string targetEnv = "dev", string dataFactoryName = null, bool isChangeContent = false)
        {
            foreach (PipelineResource pr in prs)
            {
               
                if (pr.Folder.Name == "dw_dev/dm" || pr.Folder.Name == "dw_dev/edw" || pr.Folder.Name == "dw_dev/stg")
                { 
                    ClonePipeline(pr.Name, targetEnv, dataFactoryName, isChangeContent);
                }
                else
                {
                    Print(InfoType.DEBUG, $"Skip folder {pr.Folder.Name}");
                }
            }
        }

        public List<DatasetResource> ListDatasetResources(string dataFactoryName = null)
        {
            dataFactoryName = dataFactoryName ?? defaultDataFactoryName;
            var pg = client.Datasets.ListByFactory(AzureToolkit.adfResourceGroup, dataFactoryName);

            List<DatasetResource> drs = new List<DatasetResource>();

            foreach (DatasetResource datasetResource in pg)
            {
                drs.Add(datasetResource);
            }

            // Read all pages      
            while (pg.NextPageLink != null)
            {
                pg = client.Datasets.ListByFactoryNext(pg.NextPageLink);
                foreach (var datasetResource in pg)
                {
                    drs.Add(datasetResource);
                }
            }
            return drs;

        }

        private DatasetResource GetDatasetResource(string datasetName, string dataFactoryName = null)
        {
            dataFactoryName = dataFactoryName ?? defaultDataFactoryName;

            DatasetResource dr = client.Datasets.Get(AzureToolkit.adfResourceGroup, dataFactoryName, datasetName);
            return dr;
        }

        public void CloneDataset(string dataSetName, string targetEnv = "dev", string dataFactoryName = null)
        {
            string sourceEnv = "dev";

            // Check if dataset name start with source env
            if (!dataSetName.StartsWith(sourceEnv + "_"))
            {
                Print(InfoType.WARN, $"Dataset {dataSetName} does not start with [{sourceEnv}_], skipped clone!");
                return;
            }

            dataFactoryName = dataFactoryName ?? defaultDataFactoryName;

            try
            {
                DatasetResource dr = GetDatasetResource(dataSetName, dataFactoryName);

                // Change dataset linked service
                string sourceLinkedServiceName = dr.Properties.LinkedServiceName.ReferenceName;
                string targetLinkedServiceName = sourceLinkedServiceName.Replace("dw_" + sourceEnv, "dw_" + targetEnv);
                LinkedServiceReference targetLinkedService = new LinkedServiceReference(targetLinkedServiceName);

                dr.Properties.LinkedServiceName = targetLinkedService;

                // Change dataset name
                string targetDatasetName = dr.Name.Replace(sourceEnv + "_", targetEnv + "_");

                // Change dataset folder name

                string sourceFolder = dr.Properties.Folder.Name;
                string targetFolder = sourceFolder.Replace("dw_" + sourceEnv, "dw_" + targetEnv);
                dr.Properties.Folder = new DatasetFolder(targetFolder);

                client.Datasets.CreateOrUpdate(AzureToolkit.adfResourceGroup, dataFactoryName, targetDatasetName, dr);

                Print(InfoType.INFO, $"Clone dataset {dataSetName} to {targetDatasetName}");
            }
            catch (Exception ex)
            {
                Print(InfoType.ERROR, $"Create or Update dataset [{dataSetName}] failed");
                Print(InfoType.ERROR, ex.Message);
                return;
            }

        }

        public void CloneDatasets(List<DatasetResource> drs, string targetEnv = "dev", string dataFactoryName = null)
        {
            foreach (DatasetResource dr in drs)
            {

                if (dr.Properties.Folder.Name == "dw_dev/source" || dr.Properties.Folder.Name == "dw_dev/edw" || dr.Properties.Folder.Name == "dw_dev/stg")
                {
                    CloneDataset(dr.Name, targetEnv, dataFactoryName);
                }
                else
                {
                    Print(InfoType.DEBUG, $"Skip folder {dr.Properties.Folder.Name}");
                }
            }
        }

        public PipelineRunsQueryResponse GetMainPipelineLog(string dataFactoryName, IList<string> pipelines)
        {
            Print(InfoType.INFO, "Geting main pipeline log...");

            string[] successStatus = { "Succeeded" };

            RunQueryFilter[] condition = {new RunQueryFilter("PipelineName", "In", pipelines), new RunQueryFilter("Status", "In", successStatus) }; 
            RunFilterParameters filterParams = new RunFilterParameters(DateTime.Today, DateTime.Now.AddDays(1), filters: condition);

            //Get a set of PipelineRun
            PipelineRunsQueryResponse res = client.PipelineRuns.QueryByFactory(AzureToolkit.adfResourceGroup, dataFactoryName, filterParams);

            return res;
        }

        public PipelineRunsQueryResponse GetAbnormalPipelineLog(string dataFactoryName, IList<string> pipelines)
        {
            Print(InfoType.INFO, "Geting abnormal pipeline log...");

            string[] abnormalStatus = { "Failed", "InProgress", "Canceling", "Cancelled", "Queued" };

            RunQueryFilter[] condition = { new RunQueryFilter("Status", "In", abnormalStatus), new RunQueryFilter("Status", "NotIn", pipelines) };
            RunFilterParameters filterParams = new RunFilterParameters(DateTime.Today, DateTime.Now.AddDays(1), filters: condition);

            //Get a set of PipelineRun
            PipelineRunsQueryResponse res = client.PipelineRuns.QueryByFactory(AzureToolkit.adfResourceGroup, dataFactoryName, filterParams);

            return res;
        }

        public void SendADFLogCheckMail(PipelineRunsQueryResponse mainPipelineRes, PipelineRunsQueryResponse abnormalPipelineRes,string smtpHost, string smtpUser, string smtpPassword, string mailFrom, string mailTo, string mailCc = null, string mailSubject = "Daily Check", string mailTemplateFile = @"Resource\mail.html")
        {

            string mailBodyTemplate = File.ReadAllText(mailTemplateFile, encoding: Encoding.UTF8); ;

            string tableTemplate = @"<br/>
                <table border=""1"" cellpadding=""5"" cellspacing=""0"" height=""68"" style=""border-collapse: collapse;"" >
                <tbody>
                <tr height=""21"" style=""height:15.6pt"">
                <td class=""xl63"" height=""21"" style=""height:15.6pt;width:96pt"" width=""130""><strong><span style=""font-size:14px""><span style=""font-family:arial,helvetica\ neue,helvetica,sans-serif"">Pipeline</span></span></strong></td>
                <td class=""xl63"" style=""border-left:none;width:96pt"" width=""150""><strong><span style=""font-size:14px""><span style=""font-family:arial,helvetica\ neue,helvetica,sans-serif"">Start Time</span></span></strong></td>
                <td class=""xl63"" style=""border-left:none;width:96pt"" width=""150""><strong><span style=""font-size:14px""><span style=""font-family:arial,helvetica\ neue,helvetica,sans-serif"">End Time</span></span></strong></td>
                <td class=""xl63"" style=""border-left:none;width:96pt"" width=""120""><strong><span style=""font-size:14px""><span style=""font-family:arial,helvetica\ neue,helvetica,sans-serif"">Job Status</span></span></strong></td>
                </tr>
	                {0}
                </tbody>
                </table>
                ";

            string tableBodyTemplate = @"<tr height=""21"" style=""height:15.6pt"">
	            <td align=""left"" class=""xl64"" height=""21"" style=""height:15.6pt;border-top:none""><span style=""font-family:arial,helvetica neue,helvetica,sans-serif"">{0}</span></td>
	            <td align=""left"" class=""xl64"" style=""border-top:none;border-left:none""><span style=""font-family:arial,helvetica neue,helvetica,sans-serif"">{1}</span></td>
	            <td align=""left"" class=""xl64"" style=""border-top:none;border-left:none""><span style=""font-family:arial,helvetica neue,helvetica,sans-serif"">{2}</span></td>
	            <td align=""left"" class=""xl64"" style=""border-top:none;border-left:none""><span style=""font-family:arial,helvetica neue,helvetica,sans-serif"">{3}</span></td>
            </tr>";
            
            string tableBodyHtml = "";

  
            foreach (PipelineRun pr in mainPipelineRes.Value)
            {
                // RunEnd of Cancelled job will be null
                string runEndTime = pr.RunEnd == null ? "" : pr.RunEnd.Value.AddHours(8).ToString();
                tableBodyHtml += string.Format(tableBodyTemplate, pr.PipelineName, pr.RunStart.Value.AddHours(8), runEndTime, pr.Status);
            }

            foreach (PipelineRun pr in abnormalPipelineRes.Value)
            {
                // RunEnd of Cancelled job will be null
                string runEndTime = pr.RunEnd == null ? "" : pr.RunEnd.Value.AddHours(8).ToString();
                tableBodyHtml += string.Format(tableBodyTemplate, pr.PipelineName, pr.RunStart.Value.AddHours(8), runEndTime, pr.Status);
            }

            string tableHtml;
            if (tableBodyHtml == "")
            {
                tableHtml = "No pipeline runs";
            }
            else
            {
                tableHtml = string.Format(tableTemplate, tableBodyHtml);
            }

            string body = mailBodyTemplate.Replace("{Text}", tableHtml);

            SmtpClient client = new SmtpClient
            {
                DeliveryMethod = SmtpDeliveryMethod.Network,
                Host = smtpHost,
                UseDefaultCredentials = true,
                Credentials = new System.Net.NetworkCredential(smtpUser, smtpPassword)
            };

            
            MailMessage msg = new MailMessage
            {
                From = new MailAddress(mailFrom)
            };

            string[] mailToList = mailTo.Split(';');
            foreach (string mailToOne in mailToList)
            {
                msg.To.Add(mailToOne);
            }
   
            if (mailCc != null)
            {
                string[] mailCcList = mailCc.Split(';');
                foreach (string mailCcOne in mailCcList)
                {
                    msg.CC.Add(mailCcOne);
                }  
            }
            
            msg.Subject = mailSubject;   
            msg.Body = body;
            msg.BodyEncoding = Encoding.UTF8;  
            msg.IsBodyHtml = true;
            //msg.Priority = MailPriority.High; 

            try
            {
                client.Send(msg);
                Print(InfoType.INFO, $"send mail success at {DateTime.Now}");
             
            }
            catch (SmtpException ex)
            {
                Print(InfoType.ERROR, ex.Message);
                Print(InfoType.ERROR, "send mail failed at {DateTime.Now}");
            }
        }

    }
}
