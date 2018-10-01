using System;
using System.Collections.Generic;
using System.IO;
using NLog;


namespace DocumentCategoryMap
{
    class LanguageMap
    {
        IDictionary<string, string> map = new Dictionary<string, string>();
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public bool LoadMap(string mapfile)
        {
            bool rc = true;
            int ic = 0;
            string line;
            try
            {
                using (StreamReader sr = new StreamReader(mapfile))
                {
                    while ((line = sr.ReadLine()) != null)
                    {

                        string[] parts = line.Split(new char[] { ',' });
                        if (parts[0] == string.Empty)
                            continue;

                        map.Add(parts[0], parts[1].ToLower());
                        ic++;
                    }
                }
            }
            catch (Exception)
            {
                rc = false;
            }
            logger.Info("Language map, loaded: {0} records", ic);
            return rc;
        }

        public string Lookup( string code )
        {
            string val = string.Empty;
            if (map.ContainsKey(code))
                val = map[code];
            return val;
        }
    }
}
