using System.IO;
using System;

using System.Collections.Generic;
using System.Linq;
using NLog;
namespace AWSConverterController
{
    /*
     * this will hold reponse mssage
     * it consists of request message id and 
     * list of files that are not processed and their ids
     * it is needed for re-processing
     */
    public class ConversionResponseMessage
    {
        public string RequestMessageId { get; set; }
        private string sep = "|";

        public string InstanceId { get; set; }

        private List<int> ids = new List<int>();
        private List<string> fileURLs = new List<string>();

        private List<string> parsedList = new List<string>();
        private int NoOfNotProcessedFiles { get; }

        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public ConversionResponseMessage( string requestMessageId)
            : this()
        {
            this.RequestMessageId = requestMessageId;
        }

        public ConversionResponseMessage()
        {
            this.NoOfNotProcessedFiles = 0;
        }

        public void AddIdAndFileUrlThatIsNotProcessed( int id, string fileUrl)
        {
            this.ids.Add(id);
            fileURLs.Add(fileUrl);
        }

        public string GetMessageBody()
        {
            string messageBody = this.RequestMessageId + sep + this.InstanceId;
            string msg = "";
            for (int i = 0; i < this.ids.Count(); i++)
            {
                msg += sep;
                msg += ids[i].ToString() + sep + fileURLs[i];
            }
            messageBody += msg;
            return messageBody;
        }
        /*
        * this one is used to parse message back 
        */
        public void ParseMessage(string msg)
        {
            this.parsedList.Clear();

            logger.Debug("In ParseMessage");
            logger.Debug(msg);

            char chrSep = this.sep.ElementAt(0);

            string[] elements = msg.Split(chrSep);
            this.RequestMessageId = elements[0];
            // elements[1] was empty before, used to put instance id
            this.InstanceId = elements[1];
            for (int i = 2; i < elements.Count(); i = i + 2)
            {
                string idStr = elements[i];
                string file_url = elements[i + 1];
                this.parsedList.Add(idStr + chrSep + file_url);
            }
            return;
        }
        public void AppendNonProcessedIdsToFile( StreamWriter ws)
        {
            foreach( string line in this.parsedList)
            {
                ws.WriteLine(line);
            }
        }
    }

     
}
