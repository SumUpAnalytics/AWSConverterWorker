using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NLog;

namespace DocumentCategoryMap
{
    public class DocumentCategoryMap
    {
        private IDictionary<string, string> ThreeLevelMap;
        private IDictionary<string, string> TwoLevelMap;

        public string Separator { get; set; } = "~";
        public string DefaultMapValue { get; set; } = "not clasified";

        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private StreamWriter NonMappedFileWriter;
        /*
         * this one is for logging of non mapped values 
         * it is not going to be used too much because it i running on worker
         * that is terminated after it finishes
         */

        public string NonMappedFileName {
            get { return NonMappedFileName; }
            set{
                NonMappedFileWriter = new StreamWriter(value);
            }
        }

        private void RecordMissingMap( int level, string key)
        {
            string outStr =
                String.Format("No {0} level mapping for key {1}", level, key);
                logger.Warn( outStr );
            if (this.NonMappedFileWriter == null)
                return;

            this.NonMappedFileWriter.WriteLine(outStr);
            this.NonMappedFileWriter.Flush();
        }


        public bool LoadThreeLevelMap( string fileName)
        {
            ThreeLevelMap = this.LoadLevelMap(fileName, 3);

            if (ThreeLevelMap.Count == 0)
                return false;

            return true;
        }

        public bool LoadTwoLevelMap(string fileName)
        {
            TwoLevelMap = this.LoadLevelMap(fileName, 2);

            if (TwoLevelMap.Count == 0)
                return false;

            return true;
        }

        private IDictionary<string, string> LoadLevelMap( string mapfile, int level)
        {
            IDictionary<string, string> map = new Dictionary<string, string>();
            string line;
            int ic = -1;
            try
            {
                using (StreamReader sr = new StreamReader(mapfile))
                {
                    while ((line = sr.ReadLine()) != null)
                    {
                        if (ic == -1)  // skip header
                        {
                            ic++;
                            continue;
                        }

                        string[] parts = line.Split(new char[] { ',' });
                        if (parts[0] == string.Empty)
                            continue;
                        ic++;
                        string key = concatinateKeys(parts.ToList().GetRange(0, level).ToArray() );
                        //  Console.WriteLine(key + "-->" + parts[level]);
                        if( ! map.ContainsKey(key) )
                            map.Add(key, parts[level]);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Info("Document category Map, error around line: {0}", ic);
                logger.Info(ex.Message);
            }
            logger.Info("Document category Map, loaded: {0} records", ic);
            return map;
        }
        /*
         * trivial key concatination
         */
        private string concatinateKeys(string[] keys)
        {
            string key = string.Join(this.Separator, keys );
            return key;
        }

        /*
         * these 3 keys are 
         * source (or bank), section and subsection 
         */
        public string Lookup( string key1, string key2, string key3)
        {
            string key = concatinateKeys(new string[] { key1, key2, key3 });
            string value = this.DefaultMapValue;

            if( this.ThreeLevelMap.ContainsKey(key))
                value = this.ThreeLevelMap[key];
            else
            {
                RecordMissingMap(3, key);
                key = concatinateKeys(new string[] { key1, key2 });
                if (this.TwoLevelMap.ContainsKey(key))
                    value = this.TwoLevelMap[key];
                else
                    RecordMissingMap(2, key);
            }
            return value;
        }
    }

    
}

 