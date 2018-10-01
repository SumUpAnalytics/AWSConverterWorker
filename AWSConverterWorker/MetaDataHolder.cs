using System;
using System.IO;
using NLog;
using System.Text;
using System.Text.RegularExpressions;
/*
 * select source, section, subsection, language, title,  file_original_url, parsed_date
 * from bank_data
 * -- where id in (
 * 
 * This is class that will hold all data for the document and it will be used to generate json file 
 * with all attributes 
 * 
 * 
 * 
 */
namespace DocumentCategoryMap
{
    class MetaDataHolder
    {
        public string Time { get; set; }                    // date/time when document is created 
        public string Title { get; set; }                   // title of the document
        public string Bank { get; set; }                    // financial institiution 
        public string Language { get; set; }                // language of the document english, italian ....
        public string DocumentCategory { get; set; }        // document category 
        public string Url { get; set; }                     // original document url  
        public string S3Url { get; set;  }                  // s3 url of the pdf file 
        public string TimePeriond { get; set; }             // today - Time but leave it empty
        public StringBuilder Content { get; set; }          // content 
        public int BankDataTableId { get; set; }            // id in bank_data table 
        public string S3region { get; set; }                // s3 region 
        public string S3bucket { get; set; }                // s3 bucket 

        private static string quote = '"'.ToString();
        private static string quoteComma = quote + @",";
        private static string quoteEscaped = @"\" + quote;

        public Logger logger = NLog.LogManager.GetCurrentClassLogger();

        /*
         * escape characters that are problem for json 
         * for details see http://www.ietf.org/rfc/rfc4627.txt
         */
        private string EscapeForJSON(string line)
        {
            string newLine = Regex.Replace(line, @"\\", @"\\");
            newLine = Regex.Replace(newLine, quote, quoteEscaped);
            newLine = Regex.Replace(newLine, "\t", "\\t");
            newLine = Regex.Replace(newLine, "/", @"\/");
            newLine = Regex.Replace(newLine, "\b", @"\\b");
            newLine = Regex.Replace(newLine, "\f", @"\\f");
            newLine = Regex.Replace(newLine, "\t", @"\\t");
            return newLine;
        }

        /*
         * this one will try to eliminate some control characters 
         * it is developed mostly to resolve some problems with title
         * for now removes cr/lf and trims blanks at the end 
         */
        private string RemoveSomeControlChars( string line )
        {
            string newLine = Regex.Replace(line, @"\r\n?|\n", "");
            newLine = Regex.Replace(newLine, @"\n", "");
            newLine = Regex.Replace(newLine, @"\r", "");
            return newLine.TrimEnd();
        }

        public void SaveAsJSON( string fileName )
        {

            using (StreamWriter sw = new StreamWriter(fileName))
            {
                sw.WriteLine("{");
                sw.WriteLine(@"""time"": """ + EscapeForJSON(this.Time) + quoteComma);
                sw.WriteLine(@"""title"": """ + EscapeForJSON(RemoveSomeControlChars(this.Title)) + quoteComma);
                sw.WriteLine(@"""bank"": """ + this.Bank + quoteComma);
                sw.WriteLine(@"""bank_data_table_id"": """ + this.BankDataTableId.ToString() + quoteComma);
                sw.WriteLine(@"""language"": """ + this.Language + quoteComma);
                sw.WriteLine(@"""document_category"": """ + this.DocumentCategory + quoteComma);
                sw.WriteLine(@"""url"": """ + EscapeForJSON(this.Url) + quoteComma);
                sw.WriteLine(@"""internal_url"": """ + EscapeForJSON(this.S3Url) + quoteComma);
                sw.WriteLine(@"""time_period"": """ + this.TimePeriond + quoteComma);
                sw.WriteLine(@"""content"": """ + this.Content.ToString() + quote);
                sw.WriteLine("}");
            }
        }
        /*
         * two functions to construct s3 url from region bucket and file name 
         * in form 
         * https://s3-us-west-2.amazonaws.com/sumup-test-mm/doc-name.docx
         */
        public void ConstructS3Url(string fileName)
        {
            this.ConstructS3Url(this.S3region, this.S3bucket, fileName);

        }
        public void ConstructS3Url( string region, string bucket, string fileName)
        {
            this.S3Url = @"https://s3-" + region + ".amazonaws.com/" + bucket + "/" + fileName;
        }

        /*
         * loads content from text file 
         */
        public bool LoadContentFromFile( string fileName)
        {
            bool isok = true;
            StreamReader sr = null;
            this.Content = new StringBuilder();
            try
            {
                sr = new StreamReader(fileName);
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    line = EscapeForJSON(line);

                    this.Content.Append(line);
                    this.Content.Append(" ");
                }
                sr.Close();
            }
            catch (Exception e)
            {
                isok = false;
                logger.Error("problem reading " + fileName);
                logger.Error(e.Message);
            }
            finally
            {
                if( sr != null)
                {
                    sr.Close();
                }
            }
            return isok;
        }
    }
}

