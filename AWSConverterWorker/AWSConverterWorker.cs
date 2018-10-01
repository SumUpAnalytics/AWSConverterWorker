using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;

using Amazon;
using Amazon.S3;
using Amazon.SQS;
using Amazon.SQS.Model;

using SolidFramework.Converters.Plumbing;
using SolidFramework.Converters;

using AWSConverterController;
using RemoveTablesFromDocx;
using ConvertDocxToText1;
using DocumentCategoryMap;
using BankDataDynamoDbDAO;
using NLog;

namespace AWSConverterWorker
{
    class AWSConverterWorker
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            // imput parameters, read from config file App.config

            string toDebug = Config.toDebug;
            string pdfBucketName = Config.pdfBucketName;
            string docxBucketName = Config.docxBucketName;
            string outputDir = Config.outputDir;
            string licensePath = Config.licensePath;
            string listenerQueueName = Config.listenerQueueName;
            string confirmationQueueName = Config.confirmationQueueName;
            string extractWorkDir = Config.extractWorkDir;
            string tempDocDir = Config.tempDocDir;
            int sleepTimeMillis = Config.sleepTimeMillis;
            
            int maxIdleTime = Config.maxIdleTime;

            Config.printAllParams();

            // start logging 
            logger.Info("Starting program");

            // try to get instance id 
            string instanceId = "NOT_ON_AMAZON";
            try
            {
                instanceId = Amazon.Util.EC2InstanceMetadata.InstanceId.ToString();
                logger.Info("Instance id:" + instanceId);
            }
            catch (Exception)
            {
                logger.Info("Not on EC2 instance");
            }

            //  clear working directory 

            RemoveFilesAndSubDirectories(tempDocDir);

            // initialize various objects that we need all the time 
            RegionEndpoint bucketRegion = RegionEndpoint.USWest1;
            IAmazonS3 s3Client = new AmazonS3Client(bucketRegion);

            RegionEndpoint docxBucketRegion = RegionEndpoint.USWest2;
            IAmazonS3 s3DocxClient = new AmazonS3Client(docxBucketRegion);

            MetaDataHolderFactory.connectionString = Config.DbConnectionString;
            MetaDataHolderFactory.loadMaps(Config.languageMapFile, Config.threeMapFile, Config.twoMapFile, Config.nonMapFile);
            MetaDataHolderFactory.S3bucket = pdfBucketName;
            // text is needed like us-west-2
            MetaDataHolderFactory.S3region = Amazon.RegionEndpoint.USWest1.SystemName;

            BankDataProcessingDynamoDbDAO bankDataProcessing =
                new BankDataProcessingDynamoDbDAO(Amazon.RegionEndpoint.USWest2.SystemName, pdfBucketName, docxBucketName);

            char delimiter = '|';

            // open queues 
            // connect to sending queue
            AmazonSQSConfig sqsConfig = new AmazonSQSConfig();
            // this is needed as well 
            sqsConfig.RegionEndpoint = Amazon.RegionEndpoint.USWest2;
            AmazonSQSClient sqsClient = new AmazonSQSClient(sqsConfig);

            ReceiveMessageRequest recRequest = new ReceiveMessageRequest();
            recRequest.QueueUrl = listenerQueueName;
            recRequest.MaxNumberOfMessages = 1;

            // loop and read mails from controller 
            int counter = 0;
            // doWork will be true until message with id = 0 is detected
            // this part is not implemented yet !!!
            bool doWork = true;

            // get message time, this will be the time of last message sent 
            // if nothing happens for some time program will exit

            DateTime lastMessageTime = DateTime.Now;

            while ( doWork )
            {
                TimeSpan idleTime = DateTime.Now.Subtract(lastMessageTime);
                if (idleTime.TotalMinutes > maxIdleTime)
                {
                    logger.Info("Exiting, no message within last " + maxIdleTime.ToString() + " minutes");
                    break;
                }
                List<Tuple<int, string>> tuples;

                ConversionResponseMessage conversionResponseMessage;
                string requestMessageId;

                WaitForInputMessage(listenerQueueName, delimiter, sqsClient, recRequest, out requestMessageId, out tuples);
                // now process documents one after another
                int processedCounter = 0, totalCounter = 0;

                conversionResponseMessage = new ConversionResponseMessage(requestMessageId);
                conversionResponseMessage.InstanceId = instanceId;
                if (tuples == null)
                {
                    logger.Debug("Sleeping");
                    System.Threading.Thread.Sleep(sleepTimeMillis);
                    continue;
                }

                MetaDataHolderFactory.GetConnection();
                bool ok = bankDataProcessing.Connect();
                if (!ok)
                {
                    logger.Error("Error in connecting to dynamo db: " );
                    System.Environment.Exit(1);
                }

                foreach (Tuple<int, string> tup in tuples)
                {
                    totalCounter++;
                    int id = tup.Item1;
                    string fileUrl = tup.Item2;
                    if (id == 0)
                    {
                        doWork = false;
                        break;
                    }
                    logger.Info(counter + " processing id: " + id + " " + fileUrl);

                    // now do the processing of database data for id
                    List<MetaDataHolder> mhlist = MetaDataHolderFactory.PopulateMetaDataHoldersFromDb(new int[] { id });

                    // it is always just one meta data holder for now so we can easily extract it
                    // and pass it to the processing routine 
                    MetaDataHolder holder = mhlist[0];
                    string textFileS3Path = "";
                    ok = DoFileConversion(s3Client, pdfBucketName, s3DocxClient, docxBucketName,
                        licensePath, outputDir, extractWorkDir, tempDocDir, fileUrl, holder, out textFileS3Path );
                    if (!ok)
                    {
                        logger.Error("Error in processing id: " + id.ToString());
                        conversionResponseMessage.AddIdAndFileUrlThatIsNotProcessed(id, fileUrl);
                        continue;
                    }
                    else
                    {
                        bankDataProcessing.Insert(id, holder.Bank, holder.Language, fileUrl, textFileS3Path);
                        processedCounter++;
                    }
                }

                MetaDataHolderFactory.CloseConnection();
                bankDataProcessing.Disconnect();

                // processing done see how successfull and report to the controller
                int badFiles = totalCounter - processedCounter;
                if (badFiles > 0)
                {
                    logger.Info("Not all files are processed succesfully, failures:" + badFiles.ToString());
                }

                SendMessageRequest request = new SendMessageRequest();
                request.MessageBody = conversionResponseMessage.GetMessageBody();
                request.QueueUrl = confirmationQueueName;
                SendMessageResponse confirmationResponse = sqsClient.SendMessage(request);
                if (confirmationResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    logger.Debug("Confirmation message sent ");
                    // remember when last message with results is sent 
                    lastMessageTime = DateTime.Now;
                }
                else
                {
                    logger.Error("Problem sending confirmation message");
                }

            }

            System.Environment.Exit(0);
        }

        private static void WaitForInputMessage(
            string listenerQueueName, char delimiter, AmazonSQSClient sqsClient, 
            ReceiveMessageRequest recRequest, out string requestMessageId, out List<Tuple<int, string>> tuples)
        {
            logger.Info("Checking messages");

            tuples = null;
            requestMessageId = "0";

            ReceiveMessageResponse response = sqsClient.ReceiveMessage(recRequest);
            if (response.Messages.Count == 0)
            {
                 return;
            }
            requestMessageId = response.Messages[0].MessageId;
            logger.Info("processing message: " + requestMessageId);

            // will be used to delete message
            string messageReceiptHandle = response.Messages[0].ReceiptHandle;
            string body = response.Messages[0].Body;
            tuples = ConversionRequestMessage.GetIdAndFileTuples(body, delimiter.ToString());

            // delete message and dp the work 
            // delete message
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
            deleteMessageRequest.QueueUrl = listenerQueueName;
            deleteMessageRequest.ReceiptHandle = messageReceiptHandle;

            // success is not tested !!!
            sqsClient.DeleteMessage(deleteMessageRequest);
        }

        /*
        * do all the work 
        */

        public static bool DoFileConversion(IAmazonS3 s3Client, string pdfBucketName,
                                            IAmazonS3 s3DocxClient, string docxBucketName,
                                            string licensePath,
                                            string outputDir, string extractWorkDir, string tempDocDir,
                                            string objectKey, MetaDataHolder holder,
                                            out string textFileS3Path )
        {
            textFileS3Path = String.Empty;  // this is output file location 
            // we will need this array
            char delimiter = '/';
            String[] keyElements = objectKey.Split(delimiter);
            string fileName = keyElements[keyElements.Length - 1];

            string pdfFilePath = outputDir + fileName;
            logger.Debug("downloading " + objectKey + " --> " + pdfFilePath);

            Stopwatch swAll = new Stopwatch();
            swAll.Start();

            bool ok = DownloadFileFromS3(s3Client, pdfBucketName, objectKey, pdfFilePath);
            if (!ok)
            {
                logger.Error("Error while downloading");
                return ok;
            }
            // get file length
            long length = new System.IO.FileInfo(pdfFilePath).Length;

            // construct the name of the doc file 
            string docxPath = Path.ChangeExtension(pdfFilePath, ".docx");

            // start stop watch 
            Stopwatch sw = new Stopwatch();
            sw.Start();
            ok = ConvertPDFDocument(pdfFilePath, docxPath, licensePath);
            sw.Stop();
            if (!ok)
            {
                logger.Error("Error while converting");
                DeleteFile(pdfFilePath);
                return ok;
            }
            double conversionRate = (double)length / sw.Elapsed.TotalSeconds;
            logger.Info("Done conversion, size: " + length + " time:" + sw.Elapsed.TotalSeconds + " sec, rate:" + conversionRate + " b/s");

            // new filter parts of the doc that are not needed
            // this is not the best place to have it 
            DocxTagFilter filter = new DocxTagFilter(extractWorkDir);
            // set default tags
            filter.SetupDefaultTags();

            // construct the location of final output file 
            logger.Debug("Starting Filtering" );

            string newDocxFile = tempDocDir + @"\" + fileName;
            newDocxFile = Path.ChangeExtension(newDocxFile, ".docx");

            ok = filter.ApplyFilter(docxPath, newDocxFile, false);
            if (!ok)
            {
                DeleteListOfFiles(new List<string> { docxPath, pdfFilePath });
                logger.Error("Error while filtering docx");
                return ok;
            }

            // one more step convert docx to txt 
            logger.Debug("Starting extraction of the text");
            string textFileName = Path.ChangeExtension(newDocxFile, ".txt");
            DocxToText docxToText = new DocxToText();
            ok = docxToText.ExtractTextAndSave(newDocxFile, textFileName);
            if( !ok)
            {
                DeleteListOfFiles(new List<string> { docxPath, pdfFilePath, newDocxFile });
                logger.Error("Error while Extracting text");
                return ok;
            }

            /* now we have text file and we will need json file and we need to 
             * collect data from database 
             */

            ok = holder.LoadContentFromFile(textFileName);
            if (!ok)
            {
                DeleteListOfFiles(new List<string> { docxPath, pdfFilePath, newDocxFile, textFileName });
                logger.Error("Error while loading content from text file");
                return ok;
            }

            // now save json file 
            string jsonFileName = Path.ChangeExtension(newDocxFile, ".json");
            holder.SaveAsJSON(jsonFileName);

            // construct output object name 
            // we are now uploading json file not docx !!!

            string jsonS3FileName = Path.GetFileName(jsonFileName);
            Array.Resize(ref keyElements, keyElements.Length - 1);
            string jsonObjectName = string.Join(delimiter.ToString(), keyElements) + delimiter.ToString() + jsonS3FileName;
            logger.Debug("uploading " + newDocxFile + " --> " + jsonObjectName);

            ok = UploadFileToS3(s3DocxClient, docxBucketName, jsonObjectName, jsonFileName);
            if (!ok)
            {
                logger.Error("Error while uploading");
                return ok;
            }
            textFileS3Path = jsonObjectName;
            swAll.Stop();

            logger.Info("Time for the cycle:" + swAll.Elapsed.TotalSeconds + " sec");

            // all good, delete files 
            DeleteListOfFiles(new List<string> { docxPath, pdfFilePath, newDocxFile, jsonFileName, textFileName } );

            return true;
        }

        /*
         * downloads file form s3
         */
        public static bool DownloadFileFromS3(IAmazonS3 s3Client, string bucketName, string objectKey, string filePath)
        {
            IDictionary<string, object> dic = new Dictionary<string, object>();
            bool rc = true;

            try
            {
                s3Client.DownloadToFilePath(bucketName, objectKey, filePath, dic);
            }
            catch (AmazonS3Exception ex)
            {
                rc = false;
                if (ex.ErrorCode != null && (ex.ErrorCode.Equals("InvalidAccessKeyId") ||
                    ex.ErrorCode.Equals("InvalidSecurity")))
                {
                    logger.Error("Please check the provided AWS Credentials.");
                }
                else
                {
                    logger.Error("Caught Exception: " + ex.Message);
                    logger.Error("Response Status Code: " + ex.StatusCode);
                    logger.Error("Error Code: " + ex.ErrorCode);
                    logger.Error("Request ID: " + ex.RequestId);
                }
            }
            return rc;
        }

        /*
         * uploads file to s3
         */
        public static bool UploadFileToS3(IAmazonS3 s3Client, string bucketName, string objectKey, string filePath)
        {
            IDictionary<string, object> dic = new Dictionary<string, object>();
            bool rc = true;
            try
            {
                s3Client.UploadObjectFromFilePath(bucketName, objectKey, filePath, dic);
            }
            catch (AmazonS3Exception ex)
            {
                rc = false;
                if (ex.ErrorCode != null && (ex.ErrorCode.Equals("InvalidAccessKeyId") ||
                    ex.ErrorCode.Equals("InvalidSecurity")))
                {
                    logger.Error("Please check the provided AWS Credentials.");
                }
                else
                {
                    logger.Error("Caught Exception: " + ex.Message);
                    logger.Error("Response Status Code: " + ex.StatusCode);
                    logger.Error("Error Code: " + ex.ErrorCode);
                    logger.Error("Request ID: " + ex.RequestId);
                }
            }

            return rc;
        }

        /*
         * convert document
         */
        public static bool ConvertPDFDocument(string pdfPath, string docxPath, string licensePath)
        {
            // Set the location of your the file you want to convert
            // string pdfPath = @"C:\transfer\solid-conversions\one-page-c1.pdf";

            SolidFramework.License.Import(licensePath);
            bool ok = true;

            // *PDF to DOCX*//  
            using (PdfToWordConverter converter = new PdfToWordConverter())
            {
                // Add files to convert. 
                converter.AddSourceFile(pdfPath);

                //Set the preferred conversion properties 

                // Detect Headers and Footers
                converter.HeaderAndFooterMode = HeaderAndFooterMode.Remove;

                // Set a specific page range to convert
                // converter.PageRange = new SolidFramework.PageRange(new int[] { 1 });

                // Turn on Solid Documents Optical Character Recognition (OCR) for Scanned Files
                // converter.TextRecoveryEngine = TextRecoveryEngine.SolidOCR; //Only with Pro+OCR and OCR license
                // converter.TextRecoveryType = TextRecovery.Automatic;

                // Set the layout of the reconstruction (Exact for Forms)
                converter.ReconstructionMode = ReconstructionMode.Flowing;

                // Convert the File.
                converter.ConvertTo(docxPath, true);

                // Show the status of the PDF file in the Console Window
                logger.Debug("Starting conversion");
                SolidFramework.Converters.Plumbing.ConversionStatus status = converter.ConvertTo(docxPath, true);
                if (status != ConversionStatus.Success)
                {
                    logger.Error("problem converting" + status);
                    ok = false;
                }

                return ok;
            }
        }

        /*
         * just silently delete file 
         */
        public static void DeleteFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                try
                {
                    File.Delete(filePath);
                }
                catch (System.IO.IOException e)
                {
                    logger.Error("Problem deleting file: " + filePath);
                    logger.Error(e.Message);
                }
            }
        }

        public static void DeleteListOfFiles( List<string> files)
        {
            foreach (string file in files)
                DeleteFile(file);
        }

        /*
        * deleted all files and subdirectories of the directory 
        * does not delete directory itself
        * good for cleaning working directorires 
        */
        private static void RemoveFilesAndSubDirectories(string strpath)
        {
            //This condition is used to delete all files from the Directory
            foreach (string file in Directory.GetFiles(strpath))
            {
                File.Delete(file);
            }
            //This condition is used to check all child Directories and delete files
            foreach (string subfolder in Directory.GetDirectories(strpath))
            {
                RemoveFilesAndSubDirectories(subfolder);
                Directory.Delete(subfolder);
            }
        }
    }
}