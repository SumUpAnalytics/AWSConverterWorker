using System;
using NLog;

namespace AWSConverterWorker
{
    /*
    * this is helper class to retrieve paramters from app.config
    * mostly used for integers 
    */
    public static class Config
    {
        private static Logger logger = NLog.LogManager.GetCurrentClassLogger();

        // load all config parameters from App.config 
        public static string toDebug { get;  } = System.Configuration.ConfigurationManager.AppSettings["toDebug"];
        public static string pdfBucketName { get; } = System.Configuration.ConfigurationManager.AppSettings["pdfBucketName"];
        // "sumup-docx-outbound";
        public static string docxBucketName { get; } = System.Configuration.ConfigurationManager.AppSettings["docxBucketName"];
        // @"C:\transfer\solid-conversions\";
        public static string outputDir { get; } = System.Configuration.ConfigurationManager.AppSettings["outputDir"];

        // @"C:\transfer\solid-documents\license\license.xml";
        public static string licensePath { get; } = System.Configuration.ConfigurationManager.AppSettings["licensePath"];
        // @"C:\transfer\solid-conversions\convert-pdf-1.txt";

        public static string listenerQueueName { get; } = System.Configuration.ConfigurationManager.AppSettings["listenerQueueName"];
        public static string confirmationQueueName { get; } = System.Configuration.ConfigurationManager.AppSettings["confirmationQueueName"];
        // print configuration parameters 
        // added for document filtering
        public static string extractWorkDir { get; } = System.Configuration.ConfigurationManager.AppSettings["extractWorkDir"];
        public static string tempDocDir { get; } = System.Configuration.ConfigurationManager.AppSettings["tempDocDir"];
        // sleep time between messages 10 seconds default
        public static int sleepTimeMillis { get; } = Config.IntegerParameter("sleepTimeMillis", 10 * 1000);

        // idle time 15 min default 
        public static int maxIdleTime { get; } = Config.IntegerParameter("maxIdleTime", 15);


        // 3 mapping files  and non map file 

        public static string languageMapFile { get; } = System.Configuration.ConfigurationManager.AppSettings["languageMapFile"];
        public static string threeMapFile { get; } = System.Configuration.ConfigurationManager.AppSettings["threeMapFile"];
        public static string twoMapFile { get; } = System.Configuration.ConfigurationManager.AppSettings["twoMapFile"];
        public static string nonMapFile { get; } = System.Configuration.ConfigurationManager.AppSettings["nonMapFile"];

        // database connection string 

        public static string DbConnectionString { get;  } = System.Configuration.ConfigurationManager.AppSettings["DbConnectionString"];

        /*
         * get integer parameter 
         */
        public static int IntegerParameter(string name, int defaultValue)
        {
            string valAsStr = System.Configuration.ConfigurationManager.AppSettings[name];
            int intVal = defaultValue;
            if (!Int32.TryParse(valAsStr, out intVal))
            {
                logger.Error("Error conveting parameter " + name + " value: " + valAsStr);
                return defaultValue;
            }
            return intVal;
        }
        /*
         * get string parameter with default
         */
        public static string StringParameter(string name, string defaultValue)
        {
            string valAsStr = System.Configuration.ConfigurationManager.AppSettings[name];
            if (valAsStr == null)
            {
                logger.Error("Error retrieving parameter " + name + "No value");

                return defaultValue;
            }
            return valAsStr;
        }
        /*
         * print all parameters 
         */
        public static void printAllParams()
        {
            logger.Info("Input parameters");
            logger.Info(pdfBucketName);
            logger.Info(docxBucketName);
            logger.Info(outputDir);
            logger.Info(tempDocDir);
            logger.Info(extractWorkDir);
            logger.Info(licensePath);
            logger.Info(listenerQueueName);
            logger.Info(confirmationQueueName);
            logger.Info(sleepTimeMillis.ToString());
            logger.Info(maxIdleTime.ToString());

            logger.Info(languageMapFile);
            logger.Info(threeMapFile);
            logger.Info(twoMapFile);
            logger.Info(nonMapFile);

            logger.Info(DbConnectionString);
        }
    }
}
