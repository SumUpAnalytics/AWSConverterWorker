using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;
using NLog;


namespace DocumentCategoryMap

{
    /*
     * this class will do what is needed to construct instances of meta data holder class
     **/
    class MetaDataHolderFactory
    {
        // "server=liveboard.cjvgiw4swlyc.us-west-1.rds.amazonaws.com;database=sum_up;uid=yerem;pwd=sua.liveboard.2018;";
        public static string connectionString { get; set; } = "server=liveboard0913.cjvgiw4swlyc.us-west-1.rds.amazonaws.com;database=sum_up;uid=yerem;pwd=sua.liveboard.2018;";
        private static MySqlConnection cnn = null;
        private static LanguageMap languageMap = new LanguageMap();
        private static DocumentCategoryMap docMap = new DocumentCategoryMap();

        public static string S3region;
        public static string S3bucket;
        /*
         * ifnull added to avoid problems with null values in parsed date
         */
        private static string queryStart =
            "select id, source, section, subsection, " +
            "language, title, url, file_url, ifnull(parsed_date, scrapped_at) " +
            "from bank_data " +
            "where id in (";

        private static string queryEnd = " );";

        private static Logger logger = NLog.LogManager.GetCurrentClassLogger();
        
        /* methods */

        /*
         * load maps for language and category mappings 
         */
        public static void loadMaps(string languageMapFile, string categoryMapFile3, string categoryMapFile2, string nonMappedFile )
        {
            languageMap.LoadMap(languageMapFile);
            docMap.LoadThreeLevelMap(categoryMapFile3);
            docMap.LoadTwoLevelMap(categoryMapFile2);

            docMap.NonMappedFileName = nonMappedFile;

            logger.Info("Map loading done");

        }

        /*
         * this one is going to close connection 
         */
        public static void CloseConnection()
        {
            if( cnn != null)
            {
                cnn.Close();
                logger.Debug("Disconnected from database");
            }
        }

        /*
        * get sql connection
        */
        public static MySqlConnection GetConnection()
        {
            try
            {
                cnn = new MySqlConnection(connectionString);
                cnn.Open();
                logger.Debug("Connection established ");
            }
            catch (Exception ex)
            {
                logger.Debug("Can not open sql connection ! ");
                logger.Debug(ex.Message);
            }
            return cnn;
        }

        /*
         * this one is doing the work 
         */
        public static List<MetaDataHolder> loadData( int[] ids)
        {
            logger.Debug("Loading data");
            GetConnection();
            List<MetaDataHolder>  holders = PopulateMetaDataHoldersFromDb(ids);
            CloseConnection();
            return holders;
        }

        /*
         * get data from bank_data table for specific list of ids
         */
        public static List<MetaDataHolder> PopulateMetaDataHoldersFromDb(int[] ids)
        {
            List<MetaDataHolder> holders = new List<MetaDataHolder>();
 

            string[] idsAsStr = Array.ConvertAll(ids, x => x.ToString());

            string idList = String.Join(",", idsAsStr);

            string query = queryStart + idList + queryEnd;

            MySqlCommand cmd = new MySqlCommand(query, cnn);
            logger.Debug(cmd.CommandText);
            MySqlDataReader reader = cmd.ExecuteReader();


            while (reader.Read())
            {
                MetaDataHolder holder = new MetaDataHolder();
                holder.BankDataTableId = reader.GetInt32(0);
                holder.Bank = reader.GetString(1);   // source 
                holder.DocumentCategory = 
                    docMap.Lookup(reader.GetString(1), reader.GetString(2), reader.GetString(3));
                holder.Language = languageMap.Lookup(reader.GetString(4));
                holder.Title = reader.GetString(5);
                holder.Url = reader.GetString(6);
                holder.ConstructS3Url(S3region, S3bucket, reader.GetString(7)); // file_url
                holder.Time = reader.GetString(8);
                holders.Add(holder);
            }
            reader.Close();

            return holders;
        }

    }
}
