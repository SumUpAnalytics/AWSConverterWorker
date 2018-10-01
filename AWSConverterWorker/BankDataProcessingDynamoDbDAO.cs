using System;
using NLog;
using System.Collections.Generic;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon;
/*
 * based on 
 * http://dotnetliberty.com/index.php/2016/09/19/aws-dynamodb-on-net-core-getting-started/
 * https://docs.aws.amazon.com/sdkfornet1/latest/apidocs/html/T_Amazon_DynamoDB_AmazonDynamoDBClient.htm
 * this one is good
 * https://docs.aws.amazon.com/sdkfornet/v3/apidocs/items/DynamoDBv2/TTable.html
 * for queries
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryMidLevelDotNet.html
 * 
 * This is partial dao for bank_data_processing_status_v2 table in Dybamo DB
 * We will add methods as needed method 
 */
namespace BankDataDynamoDbDAO
{
    class BankDataProcessingDynamoDbDAO
    {
        public string DbTable { get; } = "bank_data_processing_status_v2";
        public string DbRegion { get; } = "us-west-2";

        public string PdfFileS3Bucket { get; set;  }
        public string TextFileS3Bucket { get; set; }
        public string ProcessingError { get; set; } = "No";

        private AmazonDynamoDBClient client;
        private Table table;
        private readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        // default constructor, use if you do not want to do insert 
        public BankDataProcessingDynamoDbDAO()
        {
        }

        public BankDataProcessingDynamoDbDAO(string region)
        {
            DbRegion = region;
        }

        // constructor good for insert 
        public BankDataProcessingDynamoDbDAO( string region, string pdfFileS3Bucket, string textFileS3Bucket )
        {
            DbRegion = region;
            PdfFileS3Bucket = pdfFileS3Bucket;
            TextFileS3Bucket = textFileS3Bucket;
        }
        public bool Connect()
        {
            AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
            // region is going by name like us-west-2;
            clientConfig.RegionEndpoint = RegionEndpoint.GetBySystemName(DbRegion);
            bool isOk = false;
            try
            {
                client = new AmazonDynamoDBClient(clientConfig);
                table = Table.LoadTable(client, DbTable);
                logger.Debug("Connection to dynamo db established");
                isOk = true;
            }
            catch (Exception ex)
            {
                logger.Error("Can not connect to Dynamo Db");
                logger.Error(ex.Message);
            }

            return isOk;
        }

        public bool Insert(int id, string source, string language, string pdfFileUrl, string textFileUrl)
        {
            bool isOk = false;

            Document itemToLoad = new Document();
            // add variable part
            itemToLoad["id"] = id;
            itemToLoad["source"] = source;
            itemToLoad["language"] = language;
            itemToLoad["pdf_file_url"] = pdfFileUrl;
            itemToLoad["text_file_url"] = textFileUrl;

            // add constant elements 
            itemToLoad["pdf_file_s3_bucket"] = PdfFileS3Bucket;
            itemToLoad["text_file_s3_bucket"] = TextFileS3Bucket;
            itemToLoad["is_converted_to_text"] = "Y";
            itemToLoad["is_sent_to_nucleus"] = "N";
            itemToLoad["processing_error"] = ProcessingError;
            try
            {
                table.PutItem(itemToLoad);
                logger.Debug("Insert ok");
                isOk = true;
            }
            catch (Exception ex)
            {
                logger.Error("Insert failed");
                logger.Error(ex.Message);
            }

            return isOk;
        }

        /* 
         * returns item by primary key, if there is no primary key 
         * nothing is returned ( null )
         */
        public Document GetItemById( int id)
        {
            Document document = table.GetItem(id);
            if (document == null)
                logger.Debug("get item returned null");
            else
            {
                logger.Debug("asked id {0} returned id {1}", id, document["id"].AsInt());
            }
            return document;
        }

        /*
         * verify that id is in the table 
         */
        public bool IsIdPresent( int id)
        {
            Document doc = this.GetItemById(id);
            if( doc == null)
            {
                return false;
            }
            return (doc["id"].AsInt() == id);
        }

        /*
        * I am not sure this is  good;
        */
        public void Disconnect()
        {
            if( client != null)
                client.Dispose();
        }
        /*
         * https://docs.aws.amazon.com/sdkfornet/v3/apidocs/items/DynamoDBv2/TQueryOperationConfig.html
         */
        public int GetMaxIdForSource(string source)
        {
            List<string> ll = new List<string>();
            ll.Add(source);

            QueryFilter f = new QueryFilter("source", QueryOperator.Equal, ll);

            QueryOperationConfig config = new QueryOperationConfig()
            {
                AttributesToGet = new List<string> { "id" },
                IndexName = "source-id-index",
                Select = SelectValues.SpecificAttributes,
                BackwardSearch = true,
                Limit = 10
            };
            Expression exp = new Expression();
            exp.ExpressionAttributeNames["#source_name"] = "source";
            exp.ExpressionAttributeValues[":v_source"] = source;
            exp.ExpressionStatement = "#source_name = :v_source";
            config.KeyExpression = exp;

            Search s = table.Query(config);

            bool ipass = false;
            int retId = 0;
            do
            {
                List<Document> docs = s.GetNextSet();
                if (s.Count > 0)
                {
                    Document d = docs[0];
                    if (!ipass)
                    {
                        retId = d["id"].AsInt();
                        ipass = true;
                    }
                }
            } while (!s.IsDone) ;
            return retId;
        }
    }
}
