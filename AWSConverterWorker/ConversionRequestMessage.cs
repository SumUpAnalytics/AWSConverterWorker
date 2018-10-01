using System;
using System.Collections.Generic;
using System.Linq;

namespace AWSConverterController
{
    /*
     * this class will be used to construct message that will be send to consumer to process
     */

    public class ConversionRequestMessage
    {
        private List<int> ids;
        private List<string> fileURLs;
        private int maxId;

        /* constructor
         */
        public ConversionRequestMessage()
        {
            ids = new List<int>();
            fileURLs = new List<string>();
        }

        /* clears the content of the message
         */
        public void Reset()
        {
            ids.Clear();
            fileURLs.Clear();
            maxId = 0;
        }

        /* add entry that has id and file_ulr
         */
        public void AddIdAndFile( int id, string file_url)
        {
            if (id > maxId) maxId = id;

            ids.Add(id);
            fileURLs.Add(file_url);
        }

        /* for verification purposes, returns number of entries
        */
        public int Size()
        {
            return ids.Count();
        }
        /*
         * get max id
         */
         public int getMaxId()
         {
            return maxId;
         }
         
        /*
         * returs message, this will be body of SQS message
         */
        public string GetMessageBody(string sep)
        {
            string msg = "";
            for (int i = 0; i < this.ids.Count(); i++) {
                if (i > 0)
                    msg += sep;
                msg += ids[i].ToString() + sep + fileURLs[i];
            }
            return msg;
        }
        /*
         * this one is used to parse message back 
         */
        public static List<Tuple<int, string>> GetIdAndFileTuples(string msg, string sep)
        {
            List<Tuple<int, string>> tupleList = new List<Tuple<int, string>>();

            char chrSep = sep.ElementAt(0);
            string[] elements = msg.Split(chrSep);
            for( int i = 0; i < elements.Count(); i = i + 2)
            {
                string idStr = elements[i];
                string file_url = elements[i + 1];
                int id = 0;
                Int32.TryParse(idStr, out id);
                tupleList.Add(new Tuple<int, string>(id, file_url) );
            }
            return tupleList;

        }




    }
}
