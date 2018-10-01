using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Xml;
using System.Xml.XPath;
using System.IO;
using NLog;

namespace RemoveTablesFromDocx
{
    /*
     * class is used to  
     */ 
    public class DocxTagFilter
    {
        public string WorkDir { get; set; }
        
        // this is internal file in docx zip that contains text of the document
        private string DocxEntryToFilter = "word/document.xml";
        // partial path to that file relative to WorkDir
        private string DocxEntryPartialFilePath = @"word\document.xml";

        private List<EntryToRemove> entriesToRemove = new List<EntryToRemove>();
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        // constructor 

        public DocxTagFilter( string workDir)
        {
            WorkDir = workDir;
        }

        /*
         * this one will set up defailt tags to search for 
         */
        public void SetupDefaultTags()
        {
            entriesToRemove.Add(
                new EntryToRemove("http://schemas.openxmlformats.org/wordprocessingml/2006/main", "tbl", @".//{0}:tbl")
            );

            entriesToRemove.Add(
                new EntryToRemove("http://schemas.openxmlformats.org/wordprocessingml/2006/main", "sdtContent",
                @".//{0}:stdContent")
            );

            entriesToRemove.Add(
                new EntryToRemove("http://schemas.openxmlformats.org/wordprocessingml/2006/main", "pict",
                @".//{0}:pict")
            );

            entriesToRemove.Add(
                new EntryToRemove("http://schemas.openxmlformats.org/wordprocessingml/2006/main", "footerReference",
                @".//{0}:footerReference")
            );
        }
        /*
         * this one will do the filtering
         * infile and outfile are parametrs
         */
         public bool ApplyFilter( string docxFile, string newDocxFile, bool debug)
        {

            bool docxmlFound = false;
            // clear working directory 

            this.RemoveFilesAndSubDirectories(WorkDir);
            // verify that we have DocxEntryToFilter 
            ZipArchive archive = ZipFile.Open(docxFile, ZipArchiveMode.Read);

            foreach (ZipArchiveEntry entry in archive.Entries)
            {
                if(debug)
                   logger.Debug(entry.FullName);

                if (entry.FullName == DocxEntryToFilter)
                {
                    docxmlFound = true;
                }

            }
            // no entry found exit with error  
            if( !docxmlFound )
            {
                archive.Dispose();
                return false;
            }
            // unzip docx file to working dir 
            archive.ExtractToDirectory(WorkDir);
            archive.Dispose();

            // construt full path to entry 
            string extractXML = WorkDir + @"\" + this.DocxEntryPartialFilePath;
            /*
             * find namespaces in document
             * and create inverse map of the namespaces
             * original is x -> http://scheme .....
             * inverse is http://scheme -> x 
             * I will use that one to add namespaces I need using keys that are in the document
             */

            XPathDocument x = new XPathDocument(extractXML);
            XPathNavigator foo = x.CreateNavigator();
            foo.MoveToFollowing(XPathNodeType.Element);
            IDictionary<string, string> mapOfNamespaces = foo.GetNamespacesInScope(XmlNamespaceScope.All);
            IDictionary<string, string> inverseMapOfNamespaces = new Dictionary<string, string>();

            // not all namespaces will be used so keep only ones you need 
            IDictionary<string, string> namespacesUsed = new Dictionary<string, string>();
            
            foreach (KeyValuePair<string, string> kv in mapOfNamespaces)
            {
                inverseMapOfNamespaces[kv.Value] = kv.Key;
            }

            // set all tags for entries you want to delete
            foreach (EntryToRemove entry in entriesToRemove)
            {
                string ns = entry.XmlNamespace;
                if (inverseMapOfNamespaces.ContainsKey(ns))
                {
                    string val = inverseMapOfNamespaces[ns];
                    entry.XmlNamespaceAbbreviation = inverseMapOfNamespaces[ns];
                    entry.PrepareFormattedXpath();
                    entry.IsFoundInNameSpace = true;
                    namespacesUsed[val] = ns;
                }
                else
                {
                    // we have to investigate what to do in this case 
                    // for now just ignore 
                    logger.Warn("no namespace:", ns);
                    entry.IsFoundInNameSpace = false; 
                }
            }

            // read xml file and try to eliminate 

            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.PreserveWhitespace = true;
            xmlDoc.Load(extractXML);

            // load all namespacess that are needed to do search 
            XmlNamespaceManager nsmgr = new XmlNamespaceManager(xmlDoc.NameTable);

            // should be this way
            // nsmgr.AddNamespace("x", "http://schemas.openxmlformats.org/wordprocessingml/2006/main");

            foreach (KeyValuePair<string, string> kv in namespacesUsed)
            {
                nsmgr.AddNamespace(kv.Key, kv.Value);
            }

            // now we can do the search for specific tags and delete them 
            foreach (EntryToRemove entry in entriesToRemove)
            {
                // skip entry with namespace that is not found
                if (!entry.IsFoundInNameSpace)
                    continue;

                XmlNodeList nList = xmlDoc.SelectNodes(entry.SearchXpath, nsmgr);
                int icount = 0;
                foreach (XmlNode node in nList)
                {
                    node.ParentNode.RemoveChild(node);
                    icount++;
                }
                if (debug)
                    logger.Debug(entry.SearchXpath + " removed " + icount.ToString() + " entries");
                xmlDoc.Save(extractXML);
            }
            
            // create new archive 
            if (File.Exists(newDocxFile))
            {
                File.Delete(newDocxFile);
            }
            bool ok = true;
            try
            {
                ZipFile.CreateFromDirectory(WorkDir, newDocxFile);
            }
            catch (Exception)
            {
                ok = false;
            }
            return ok;
        }
        /*
         * deleted all files and subdirectories of the directory 
         * does not delete directory itself
         * good for cleaning working directorires 
         */
        private void RemoveFilesAndSubDirectories(string strpath)
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
    /*
     * class that holds all the tags that have to be removed 
     * their namespaces and xpath search expression that will be used to find them 
     * more like a place holder
     */
    class EntryToRemove
    {
        public string XmlNamespace { get; set; }
        public string XmlNamespaceAbbreviation { get; set; }
        public string Tag { get; set; }
        public bool IsFoundInNameSpace { get; set; }
        public string SearchXpath { get; set; }
        public string SearchXpathFormatter { get; set; }

        // constructors 
        public EntryToRemove(string xmlNamespace, string tag)
        {
            XmlNamespace = xmlNamespace;
            Tag = tag;
            IsFoundInNameSpace = false;
        }
        public EntryToRemove(string xmlNamespace, string tag, string searchXpathFormatter)
            : this(xmlNamespace, tag)
        {
            SearchXpathFormatter = searchXpathFormatter;
        }
        // sets xpath from the formatter string, used to inject proper namespace for tag
        public string PrepareFormattedXpath()
        {
            string xpath = string.Format(this.SearchXpathFormatter, this.XmlNamespaceAbbreviation);

            SearchXpath = xpath;
            return xpath;
        }
    }
}
