using System;
using System.Text;
using System.IO.Compression;
using System.Xml;
using System.IO;
using NLog;


// this is constructed from 
// https://www.codeproject.com/Articles/20529/Using-DocxToText-to-Extract-Text-from-DOCX-Files

namespace ConvertDocxToText1
{
    public class DocxToText
    {
        private const string ContentTypeNamespace =
            @"http://schemas.openxmlformats.org/package/2006/content-types";

        private const string WordprocessingMlNamespace =
            @"http://schemas.openxmlformats.org/wordprocessingml/2006/main";

        private const string DocumentXmlXPath =
            @"/t:Types/t:Override[@ContentType=""" +
        "application/vnd.openxmlformats-officedocument." +
        "wordprocessingml.document.main+xml\"]";

        private const string BodyXPath = "/w:document/w:body";

        private string docxFile = "";
        private string docxFileLocation = "";

        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public DocxToText(string fileName)
        {
            docxFile = fileName;
        }

        // default empty constructor 
        public DocxToText()
        {
        }

        #region ExtractText()
        /*
         * this is method good for massive usage
         * create instance and change files names 
         */

        public bool ExtractTextAndSave( string fileName, string textFileName )
        {
            bool isOk = true;
            docxFile = fileName;
            try
            {
                string text = this.ExtractText();
                isOk = this.SaveTextFile(textFileName, text);
            }
            catch (Exception ex)
            {
                isOk = false;
                logger.Error("Problem converting to text");
                logger.Error(ex.Message);
            }

            return isOk;
        }
        /// 
        /// Extracts text from the Docx file.
        /// 
        /// Extracted text.
        public string ExtractText()
        {
            if (string.IsNullOrEmpty(docxFile))
                throw new Exception("Input file not specified.");

            // Usually it is "/word/document.xml"

            docxFileLocation = FindDocumentXmlLocation();
            logger.Info("docx file:" + docxFileLocation);
            if (string.IsNullOrEmpty(docxFileLocation))
                throw new Exception("It is not a valid Docx file.");

            return ReadDocumentXml();
        }
        #endregion

        #region FindDocumentXmlLocation()
        /// 
        /// Gets location of the "document.xml" zip entry.
        /// 
        /// Location of the "document.xml".
        private string FindDocumentXmlLocation()
        {
            ZipArchive zip = ZipFile.OpenRead(docxFile);
            foreach (ZipArchiveEntry entry in zip.Entries)
            {
                // Find "[Content_Types].xml" zip entry

                if (string.Compare(entry.Name, "[Content_Types].xml", true) == 0)
                {
                    //Stream contentTypes = zip.GetInputStream(entry);
                    Stream contentTypes = entry.Open();

                    XmlDocument xmlDoc = new XmlDocument();
                    xmlDoc.PreserveWhitespace = true;
                    xmlDoc.Load(contentTypes);
                    contentTypes.Close();

                    //Create an XmlNamespaceManager for resolving namespaces

                    XmlNamespaceManager nsmgr =
                        new XmlNamespaceManager(xmlDoc.NameTable);
                    nsmgr.AddNamespace("t", ContentTypeNamespace);

                    // Find location of "document.xml"

                    XmlNode node = xmlDoc.DocumentElement.SelectSingleNode(
                        DocumentXmlXPath, nsmgr);

                    if (node != null)
                    {
                        string location =
                            ((XmlElement)node).GetAttribute("PartName");
                        return location.TrimStart(new char[] { '/' });
                    }
                    break;
                }
            }
            zip.Dispose();

            return null;
        }
        #endregion

        #region ReadDocumentXml()
        /// 
        /// Reads "document.xml" zip entry.
        /// 
        /// Text containing in the document.
        private string ReadDocumentXml()
        {
            StringBuilder sb = new StringBuilder();

            // ZipFile zip = new ZipFile(docxFile);
            ZipArchive zip = ZipFile.OpenRead(docxFile);

            string backslashFileLocation = docxFileLocation.Replace("/", @"\");
            foreach (ZipArchiveEntry entry in zip.Entries)
            {
                if (string.Compare(entry.FullName, backslashFileLocation, true) == 0)
                {
                    Stream documentXml = entry.Open();
                    if (documentXml == null)
                    {
                        logger.Error("Stream is null");
                    }
                    
                    XmlDocument xmlDoc = new XmlDocument();
                    xmlDoc.PreserveWhitespace = true;
                    xmlDoc.Load(documentXml);
                    documentXml.Close();

                    XmlNamespaceManager nsmgr =
                        new XmlNamespaceManager(xmlDoc.NameTable);
                    nsmgr.AddNamespace("w", WordprocessingMlNamespace);

                    XmlNode node =
                        xmlDoc.DocumentElement.SelectSingleNode(BodyXPath, nsmgr);
                    if (node == null)
                    {
                        return string.Empty;
                    }
                    sb.Append(ReadNode(node));

                    break;
                }
            }
            zip.Dispose();
            return sb.ToString();
        }
        #endregion

        #region ReadNode()
        /// 
        /// Reads content of the node and its nested childs.
        /// 
        /// XmlNode.
        /// Text containing in the node.
        private string ReadNode(XmlNode node)
        {
            if (node == null || node.NodeType != XmlNodeType.Element)
                return string.Empty;

            StringBuilder sb = new StringBuilder();
            foreach (XmlNode child in node.ChildNodes)
            {
                if (child.NodeType != XmlNodeType.Element) continue;
                
                switch (child.LocalName)
                {
                    case "t":                           // Text
                        sb.Append(child.InnerText.TrimEnd());
                        sb.Append(' ');                 // this is added to put space 

                        // this is not working as expected I think but left it here 
                        string space =
                            ((XmlElement)child).GetAttribute("xml:space");
                        if (!string.IsNullOrEmpty(space) &&
                            space == "preserve")
                            sb.Append(' ');

                        break;

                    case "cr":                          // Carriage return
                    case "br":                          // Page break
                        sb.Append(Environment.NewLine);
                        break;

                    case "tab":                         // Tab
                        sb.Append("\t");
                        break;

                    case "p":                           // Paragraph
                        sb.Append(ReadNode(child));
                        sb.Append(Environment.NewLine);
                        sb.Append(Environment.NewLine);
                        break;

                    default:
                        sb.Append(ReadNode(child));
                        break;
                }
            }
            return sb.ToString();
        }
        #endregion

        #region SaveTextFile( string fileName, string text )
        /*
         * don't save lines with blanks and lines with numbers only
         */
        public bool SaveTextFile(string fileName, string text)
        {
            bool ok = true;
            System.IO.TextWriter writer = null;
            try
            {
                writer = new StreamWriter(fileName);

                string[] lines = text.Split(new[] { Environment.NewLine }, StringSplitOptions.None);
                foreach (string line in lines)
                {
                    if (line.Length > 0)
                    {
                        if (StringFilter.IsAlmostNuberOnlyString(line))
                            continue;
                        if (StringFilter.IsBlankLineOnly(line))
                            continue;
                        writer.WriteLine(line);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error("Problem saving text file");
                logger.Error(ex.Message);
                ok = false;
            }
            finally
            {
                writer.Close();
            }
            return ok;
        }

        #endregion

        #region Wordcounter()

        public long Wordcounter( string fileName)
        {
            StreamReader sr = new StreamReader(fileName);

            long counter = 0;
            string delim = " ,."; //maybe some more delimiters like ?! and so on
            string[] fields = null;
            string line = null;

            while (!sr.EndOfStream)
            {
                line = sr.ReadLine(); //each time you read a line you should split it into the words
                line.Trim();
                fields = line.Split(delim.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                counter += fields.Length; //and just add how many of them there is
            }


            sr.Close();
            return counter;
        }
        #endregion
    }
}