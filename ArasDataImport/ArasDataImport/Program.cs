using Aras.IOM;
//using Aras.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Globalization;

namespace ArasDataImport
{
    class Program
    {
        static System.IO.StreamWriter log;
        static System.IO.StreamWriter tech_log;
        static System.IO.StreamWriter error_File;
        static bool is_error = false;
        static bool Delete_Files_OnRevise_bool = true;
        static Item ItemtypeObject = null;
        static String dateformat = "";

        static Innovator inn = null;
        static bool hasNative = false;
        static int indexofNativeFile = 0;
        static bool LineHasError = false;

        static void Main(string[] args)
        {
            String Username = null, password = null, serverurl = null, db = null, inputfile = null, errorfile = null, logfile = null, techlogfile = null;

            String argstr = string.Join(",", args);

            DateTime startDate = DateTime.Now;

            if (
                argstr.ToString().Contains("-h") || argstr.ToString().Contains("-H") ||
                argstr.ToString().ToLower().Contains("-help"))
                {
                    helpContent();
                    
                }

            foreach (string arg in args)
            {
                if (arg.ToString().Contains("-user"))
                {
                    Username = arg.Split('=')[1].ToString();
                }
                if (arg.ToString().Contains("-p"))
                {
                    password = arg.Split('=')[1].ToString();
                }
                if (arg.ToString().Contains("-url"))
                {
                    serverurl = arg.Split('=')[1].ToString();
                }
                if (arg.ToString().Contains("-db"))
                {
                    db = arg.Split('=')[1].ToString();
                }
                if (arg.ToString().Contains("-input_file"))
                {
                    inputfile = arg.Split('=')[1].ToString();
                }
                if (arg.ToString().Contains("-error_file"))
                {
                    errorfile = arg.Split('=')[1].ToString();
                }
                if (arg.ToString().Contains("-log_file"))
                {
                    logfile = arg.Split('=')[1].ToString();
                }
                if (arg.ToString().Contains("-tech_file"))
                {
                    techlogfile = arg.Split('=')[1].ToString();
                }

            }

            if (String.IsNullOrEmpty(Username) || String.IsNullOrEmpty(password) || String.IsNullOrEmpty(serverurl) || String.IsNullOrEmpty(inputfile) || String.IsNullOrEmpty(errorfile) || String.IsNullOrEmpty(techlogfile) || String.IsNullOrEmpty(logfile))
            {
                helpContent();
                return;
            }
            if (!File.Exists(techlogfile))
            {
                tech_log = new StreamWriter(techlogfile);
            }
            else
            {
                tech_log = File.AppendText(techlogfile);
            }

            if (!File.Exists(logfile))
            {
                log = new StreamWriter(logfile);
            }
            else
            {
                log = File.AppendText(logfile);
            }

            //if (!File.Exists(errorfile))
            {
                error_File = new StreamWriter(errorfile);
            }
            //else
            //{
            //    error_File = File.AppendText(errorfile);
            //}

            HttpServerConnection conn = Program.login(serverurl,db,Username,password);

            if (conn != null)
            {
                tech_log.WriteLine("Login Successfully");
                inn = IomFactory.CreateInnovator(conn);
            }
            else
            {
                return;
            }
            try
            {
                processImportingData(inputfile, logfile, techlogfile, errorfile);
            }
            catch
            {

            }


            DateTime endDate = DateTime.Now;
            var seconds = System.Math.Abs((startDate - endDate).TotalSeconds);

            Console.WriteLine("Time taken for process -- " + seconds + " seconds");


            Program.logout(conn);
            tech_log.WriteLine("Logout Successfully");
            tech_log.Close();
            log.Close();
            error_File.Close();

            if (!is_error)
            {
                if (File.Exists(errorfile))
                {
                    File.Delete(errorfile);
                }
            }
        }

        private static void processImportingData(string inputfile, string logfile, string techlogfile, string errorfile)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<< Starting the Function .. processImportingData >>>>>>>>>>>>>>>>>>>>>>>>");

            Console.WriteLine("Reading config data");
            tech_log.WriteLine("Reading config data");
            log.WriteLine("Reading config data");


            String ItemType = ConfigurationManager.AppSettings["ItemType"];
            Console.WriteLine("ItemType = " + ItemType);
            tech_log.WriteLine("ItemType = " + ItemType);
            log.WriteLine("ItemType = " + ItemType);
            
            String RelationType = ConfigurationManager.AppSettings["Relation"];
            Console.WriteLine("RelationType = " + RelationType);
            tech_log.WriteLine("RelationType = " + RelationType);
            log.WriteLine("RelationType = " + RelationType);

            String FilesFolder = ConfigurationManager.AppSettings["FilesFolder"];
            Console.WriteLine("FilesFolder = " + FilesFolder);
            tech_log.WriteLine("FilesFolder = " + FilesFolder);
            log.WriteLine("FilesFolder = " + FilesFolder);

            String delimiter = ConfigurationManager.AppSettings["delimiter"];
            if (delimiter == null || delimiter == "")
            {
                delimiter = "~";
            }
            Console.WriteLine("delimiter = " + delimiter);
            tech_log.WriteLine("FilesFolder = " + FilesFolder);
            log.WriteLine("FilesFolder = " + FilesFolder);

            String defaultPropSQL ="";


            String null_properties = ConfigurationManager.AppSettings["null_properties"];
            if (!String.IsNullOrEmpty(null_properties))
            {
                string[] nullPropList = null_properties.Split(',');
                foreach(string nullProp in nullPropList)
                {
                    if (string.IsNullOrEmpty(defaultPropSQL))
                    {
                        defaultPropSQL += nullProp + "=''";
                    }
                    else
                    {
                        defaultPropSQL += "," + nullProp + "=''";
                    }
                }                 
            }


            String default_properties = ConfigurationManager.AppSettings["default_properties"];
            if (!String.IsNullOrEmpty(default_properties))
            {
                string[] defaPropList = default_properties.Split('~');
                foreach (string defaProp in defaPropList)
                {
                    string[] defaPropValues = defaProp.Split(':');
                    if (string.IsNullOrEmpty(defaultPropSQL))
                    {
                        defaultPropSQL += defaPropValues[0].ToString() + "= '" + defaPropValues[1].ToString() + "'";
                    }
                    else
                    {
                        defaultPropSQL += "," + defaPropValues[0].ToString() + "= '" + defaPropValues[1].ToString() + "'";
                    }
                }
            }


            String Delete_Files_OnRevise = ConfigurationManager.AppSettings["Delete_Files_OnRevise"];
            
            if (Delete_Files_OnRevise == "f" || Delete_Files_OnRevise == "false" || Delete_Files_OnRevise == "0")
            {
                Delete_Files_OnRevise_bool = false;
            }
            if (Delete_Files_OnRevise == "t" || Delete_Files_OnRevise == "true" || Delete_Files_OnRevise == "1")
            {
                Delete_Files_OnRevise_bool = true;
            }
            Console.WriteLine("delimiter = " + delimiter);
            tech_log.WriteLine("FilesFolder = " + FilesFolder);
            log.WriteLine("FilesFolder = " + FilesFolder);

            dateformat = ConfigurationManager.AppSettings["date_format"];

            ItemtypeObject = GetItemType(ItemType);
            
            try
            {
                ProcessImportFile(inputfile, ItemType, RelationType, FilesFolder, errorfile, delimiter, defaultPropSQL);
            }
            catch
            {
            }
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< End the Function .. processImportingData >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        }

        private static Item GetItemType(string ItemType)
        {

            String AMLStr = "<AML>" +
                              "<Item type='ItemType' action='get' where=\"[ItemType].name='" + ItemType + "'\" select='*'>" +
                                "<Relationships>" +
                                  "<Item type='Property' action='get' select='name,data_type,data_source'>" +
                                  "</Item>" +
                                  "<Item type='RelationshipType' action='get' select='*'>" +
                                    "<related_id>" +
                                      "<Item type='ItemType' action='get' select='*'>" +
                                        "<Relationships>" +
                                          "<Item type='Property' action='get' select='name,data_type,data_source'>" +
                                          "</Item>" +
                                        "</Relationships>" +
                                      "</Item>" +
                                    "</related_id>" +
                                  "</Item>" +
                                "</Relationships>" +
                              "</Item>" +
                            "</AML>";

            Item result = inn.applyAML(AMLStr);

            return result;

            throw new NotImplementedException();
        }

        private static void ProcessImportFile(string inputfile, string ItemType, string RelationType, string FilesFolder, string errorfile, string delimiter, string defaultPropSQL)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< Start the Function .. ProcessImportFile >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            int LINE_COUNT = 0;
            int Error_COUNT = 0;
            try
            {

                string[] columns = null;

                string[] lines = System.IO.File.ReadAllLines(inputfile);



                // Display the file contents by using a foreach loop.
                Console.WriteLine("Reading Input File .....");
                log.WriteLine("Reading Input File .....");
                tech_log.WriteLine("Reading Input File .....");
                
                

                char delimiterchar = delimiter.ToCharArray()[0];
                foreach (string line in lines)
                {
                    LineHasError = false;
                    // Use a tab to indent each line of the file.
                    //Console.WriteLine("\t" + line);
                    if (LINE_COUNT == 0)
                    {
                        Console.WriteLine("reading the header info... ");
                        log.WriteLine("reading the header info... ");
                        tech_log.WriteLine("reading the header info... ");


                        //Console.WriteLine("the Delimiter specified is " + delimiter.ToCharArray()[0].ToString());
                        //log.WriteLine("the Delimiter specified is " + delimiter.ToCharArray()[0].ToString());
                        //tech_log.WriteLine("the Delimiter specified is " + delimiter.ToCharArray()[0].ToString());
                        Console.WriteLine("\t" + line);
                        log.WriteLine("\t" + line);
                        tech_log.WriteLine("\t" + line);
                        columns = line.Split(delimiterchar);
                        error_File.WriteLine(line);

                        hasNative = columns.Contains("native_file");
                        if (hasNative)
                        {
                            indexofNativeFile = Array.IndexOf(columns, "native_file");
                        }

                    }
                    else
                    {
                        try
                        {
                            Console.WriteLine("\t[" + LINE_COUNT + "]" + line);
                            log.WriteLine("\t" + line);
                            tech_log.WriteLine("\t" + line);
                            string[] values = line.Split(delimiterchar);
                            processValues(ItemType, RelationType, FilesFolder, columns, values, defaultPropSQL);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            is_error = true;
                            error_File.WriteLine(line);
                            Error_COUNT++;
                            LineHasError = false;
                        }

                        if (LineHasError)
                        {
                            error_File.WriteLine(line);
                            Error_COUNT++;
                        }
                    }



                    


                    LINE_COUNT++;
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("No pf Lines Processed :- " + LINE_COUNT);
            log.WriteLine("No pf Lines Processed :- " + LINE_COUNT);
            tech_log.WriteLine("No pf Lines Processed :- " + LINE_COUNT);

            Console.WriteLine("No of Lines Failes :- " + Error_COUNT);
            log.WriteLine("No of Lines Failes :- " + Error_COUNT);
            tech_log.WriteLine("No of Lines Failes :- " + Error_COUNT);

            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< End the Function .. ProcessImportFile >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        }

        private static void processValues(string ItemType, string RelationType, string FilesFolder, string[] columns, string[] values, string defaultPropSQL)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< Start the Function .. processValues >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            try
            {

                String SQLnativefileExt = "";
                //String getRevAML = "<Item type='" + ItemType + "' action ='get' where=\"[" + ItemType + "].item_number='" + values[0] + "' and [" + ItemType + "].major_rev='" + values[1] + "' \"/>";
                //Console.WriteLine("getRevAML --> " + getRevAML);

                if (ItemType == "CAD" && hasNative)
                {
                    String NativefileExt = "";
                    //
                    String nativefileName = values[indexofNativeFile];
                    if (!string.IsNullOrEmpty(nativefileName))
                    {
                        int fileext = nativefileName.LastIndexOf('.');

                        if (File.Exists(nativefileName))
                        {
                            tech_log.WriteLine("Nativefile exist with the path as --> " + nativefileName);
                            NativefileExt = Path.GetExtension(nativefileName);
                           
                        }
                        else if (File.Exists(Path.Combine(FilesFolder, nativefileName)))
                        {
                            tech_log.WriteLine("Nativefile exist with the path as --> " + Path.Combine(FilesFolder, nativefileName));

                            NativefileExt = Path.GetExtension(Path.Combine(FilesFolder, nativefileName));
                            
                        }

                        tech_log.WriteLine("Nativefile Extension --> " + NativefileExt);
                        SQLnativefileExt = "and native_file in (select id from innovator.[file] where [FILENAME] like '%" + NativefileExt + "')";
                        tech_log.WriteLine("SQLnativefileExt --> " + SQLnativefileExt);
                    }
                }


                String getItemSQL = "Select * from innovator.[" + ItemType + "] where item_number = '" + values[0] + "' and IS_CURRENT is not null " + SQLnativefileExt + "order by MODIFIED_ON desc";

                tech_log.WriteLine("getItemSQL --> " + getItemSQL);

                Item Result = inn.applySQL(getItemSQL);
                if (Result.isError())
                {
                    Console.WriteLine("Exception while Queryng the Item" + Result.getErrorString());
                    tech_log.WriteLine("Exception while Queryng the Item" + Result.getErrorString());
                    log.WriteLine("Exception while Queryng the Item" + Result.getErrorString());
                    //throw new Exception("Exception while Queryng the Item");
                    is_error = true;
                    LineHasError = true;
                }
                Item Result_Items = Result.getItemsByXPath("//Result/Item[major_rev='" + values[1] + "']");
                if (Result_Items.getItemCount() > 0)
                {
                    UpdateExistingRevision(Result_Items, ItemType, RelationType, FilesFolder, columns, values, defaultPropSQL);
                }
                else if (Result.getItemCount() > 0 && Result_Items.getItemCount() <= 0)
                {
                    ReviseTheItem(Result, ItemType, RelationType, FilesFolder, columns, values, defaultPropSQL);
                }
                else
                {
                    AddNewItem(ItemType, RelationType, FilesFolder, columns, values, defaultPropSQL);
                    //String getItemAML 
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception while Queryng the Item" + ex.Message);
                tech_log.WriteLine("Exception while Queryng the Item" + ex.Message);
                log.WriteLine("Exception while Queryng the Item" + ex.Message);
                //throw new Exception("Exception while Queryng the Item");

                LineHasError = true;
            }
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< end the Function .. processValues >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        }

        private static void ReviseTheItem(Item Result, string ItemType, string RelationType, string FilesFolder, string[] columns, string[] values, string defaultPropSQL)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< Start the Function .. ReviseTheItem >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            try
            {
               // Item Current_Items = Result.getItemsByXPath("//Result/Item[is_cuurent='1']");
                Item CurrRevision = Result.getItemByIndex(0);
                String ItemID = CurrRevision.getProperty("id");

                Item Current_Item = inn.getItemById(ItemType, ItemID);

                //Item NewRevision = Current_Item.setAction("Revise");
                // Version and unlock the item
                Item NewRevision = Current_Item.apply("version");
                if (!NewRevision.isError())
                    NewRevision = NewRevision.apply("unlock");

                if (NewRevision != null && NewRevision.getItemCount() > 0)
                {
                    if (Delete_Files_OnRevise_bool)
                    {
                        RemoveFiles(NewRevision, RelationType);
                    }
                    UpdateExistingRevision(NewRevision, ItemType, RelationType, FilesFolder, columns, values, defaultPropSQL);
                }
                else if(NewRevision.isError())
                {
                    is_error = true;
                    LineHasError = true;
                    Console.WriteLine("\t\tError NewRevision revising the Item.." + NewRevision.getErrorString());
                    log.WriteLine("\t\tError while revising the Item.." + NewRevision.getErrorString());
                    tech_log.WriteLine("\t\tError while revising the Item.." + NewRevision.getErrorString());
                    throw new Exception();
                }

                //return resItem; 

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                log.WriteLine(ex.Message);
                tech_log.WriteLine(ex.Message);

                is_error = true;
                LineHasError = true;

                throw ex;
            }
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< End the Function .. ReviseTheItem >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");


        }

        private static void RemoveFiles(Item NewRevision, string RelationType)
        {
            try
            {
                Item Deleteitems = inn.newItem(RelationType,"get");
                Deleteitems.setProperty("source_id", NewRevision.getID());
                Deleteitems = Deleteitems.apply();

                for (int i = 0; i < Deleteitems.getItemCount(); i++)
                {
                    Item Deleteitem = Deleteitems.getItemByIndex(i);
                    Deleteitem.setAction("delete");
                    Deleteitem = Deleteitem.apply();
                    if (Deleteitem.isError())
                    {
                        is_error = true;
                        Console.WriteLine("\t\tError whle removing files.." + Deleteitem.getErrorString());
                        log.WriteLine("\t\tError whle removing files.." + Deleteitem.getErrorString());
                        tech_log.WriteLine("\t\tError whle removing files.." + Deleteitem.getErrorString());
                        throw new Exception();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw ex;
            }
        }

        private static void AddNewItem(string ItemType, string RelationType, string FilesFolder, string[] columns, string[] values, string defaultPropSQL)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< Start the Function .. AddNewItem >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            try
            {
                string state = "";

                List<string> filesList = null;
            
                Item NewItem = inn.newItem(ItemType,"add");
                NewItem.setProperty("item_number",values[0]);
                String setprop = "";

                UpdateNewItem(NewItem, ItemType, RelationType, FilesFolder, columns, values, out state, out filesList, out setprop);

                NewItem = NewItem.apply();

                if (NewItem.getItemCount() > 0)
                {

                    if (NewItem.getItemCount() == 1)
                    {
                        String updateSQLScript = "update innovator.[" + ItemType + "] set " + defaultPropSQL + "," + setprop + " where id='" + NewItem.getID() + "'";

                        tech_log.WriteLine("updateSQLScript  --> " + updateSQLScript);

                        Item sqlRes = inn.applySQL(updateSQLScript);

                        if (sqlRes.isError())
                        {
                            is_error = true;
                            LineHasError = true;
                            Console.WriteLine("Error while Updating the Major Revision as " + values[1] + "  " + sqlRes.getErrorString());
                            log.WriteLine("Error while Updating the Major Revision as " + values[1] + "  " + sqlRes.getErrorString());
                            tech_log.WriteLine("Error while Updating the Major Revision as " + values[1] + "  " + sqlRes.getErrorString());
                        }
                    }

                    //UpdateExistingRevision(NewItem, ItemType, RelationType, FilesFolder, columns, values);
                }
                else if (NewItem.isError())
                {
                    is_error = true;
                    LineHasError = true;
                    Console.WriteLine("\t\tError while Creating the Item.." + NewItem.getErrorString());
                    log.WriteLine("\t\tError while Creating the Item.." + NewItem.getErrorString());
                    tech_log.WriteLine("\t\tError while Creating the Item.." + NewItem.getErrorString());
                    throw new Exception();
                }

                if (!string.IsNullOrEmpty(state))
                {
                    string currState = NewItem.getProperty("state");
                    if (!string.IsNullOrEmpty(currState) && currState != state)
                    {
                        if (state == "Released")
                        {
                            Item NPromoteItem = NewItem.promote("In Review", "Promoted while Migration");
                            if (NPromoteItem.isError())
                            {

                                is_error = true;

                                Console.WriteLine("\t\tError while Promoting to In Review \n \t\t" + NPromoteItem.getErrorString());
                                log.WriteLine("\t\tError while Promoting to  In Review \n \t\t" + NPromoteItem.getErrorString());
                                tech_log.WriteLine("\t\tError while Promoting to  In Review \n \t\t" + NPromoteItem.getErrorString());
                                LineHasError = true;
                            }
                        }



                        Item PromoteItem = NewItem.promote(state, "Promoted while Migration");
                        if (PromoteItem.isError())
                        {

                            is_error = true;

                            Console.WriteLine("\t\tError while Promoting to  " + state + " \n \t\t" + PromoteItem.getErrorString());
                            log.WriteLine("\t\tError while Promoting to  " + state + " \n \t\t" + PromoteItem.getErrorString());
                            tech_log.WriteLine("\t\tError while Promoting to  " + state + " \n \t\t" + PromoteItem.getErrorString());
                            LineHasError = true;
                        }
                    }
                }


                foreach (string filename in filesList)
                {
                    updateFiles(NewItem, RelationType, FilesFolder, filename);
                }


                



            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                log.WriteLine(ex.Message);
                tech_log.WriteLine(ex.Message);

                is_error = true;
                LineHasError = true;
            }
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< End the Function .. AddNewItem >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");


        }

        private static void UpdateExistingRevision(Item Result, string ItemType, string RelationType, string FilesFolder, string[] columns, string[] values, string defaultPropSQL)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< Start the Function .. UpdateExistingRevision >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            try
            {
                Item CurrRevision = Result.getItemByIndex(0);
                String ItemID = CurrRevision.getProperty("id");

                String UpdateSQL = "Update innovator.[" + ItemType + "] set";
                String setprop = "";

                tech_log.WriteLine("------------------------");
                tech_log.WriteLine("No of Columns -->"+columns.Length);
                tech_log.WriteLine("No of values  -->" + values.Length);
                tech_log.WriteLine("------------------------");

                for (int inx = 1; inx < columns.Length; inx++) 
                {
                    string PropertyName = columns[inx];
                    bool isFileProperty = false;
                    string PropertyValue = getPropertyRealValue(ItemType, columns[inx], values[inx], FilesFolder, out isFileProperty);
                    tech_log.WriteLine("PropertyValue --<" + PropertyValue + "> with length <" + PropertyValue.Length + ">");
                    if (PropertyValue != null && PropertyValue != "" && PropertyValue.Length > 0)
                    {
                        if (PropertyName.ToLower() == "state")
                        {
                            string currState = CurrRevision.getProperty("state");
                            if (!string.IsNullOrEmpty(currState) && currState != PropertyValue)
                            {
                                if (PropertyValue == "Released")
                                {
                                    Item NPromoteItem = CurrRevision.promote("In Review", "Promoted while Migration");
                                    if (NPromoteItem.isError())
                                    {

                                        is_error = true;

                                        Console.WriteLine("\t\tError while Promoting to In Review \n \t\t" + NPromoteItem.getErrorString());
                                        log.WriteLine("\t\tError while Promoting to  In Review \n \t\t" + NPromoteItem.getErrorString());
                                        tech_log.WriteLine("\t\tError while Promoting to  In Review \n \t\t" + NPromoteItem.getErrorString());
                                        LineHasError = true;
                                    }
                                }


                                Item PromoteItem = CurrRevision.promote(PropertyValue,"Promoted while Migration");
                                if (PromoteItem.isError())
                                {
                                    
                                    is_error = true;

                                    Console.WriteLine("\t\tError while Promoting to  " + PropertyValue + " \n \t\t" + PromoteItem.getErrorString());
                                    log.WriteLine("\t\tError while Promoting to  " + PropertyValue + " \n \t\t" + PromoteItem.getErrorString());
                                    tech_log.WriteLine("\t\tError while Promoting to  " + PropertyValue + " \n \t\t" + PromoteItem.getErrorString());
                                    LineHasError = true;
                                    //Console.WriteLine("\t\tError while Updating the Properties");
                                    //log.WriteLine("\t\tError while Updating the Properties");
                                    //tech_log.WriteLine("\t\tError while Updating the Properties");
                                }
                            }
                        }
                        else if (!PropertyName.ToLower().Contains("file") ||(isFileProperty && PropertyName.ToLower().Contains("file")))
                        {
                            if (setprop == "")
                            {
                                setprop = PropertyName + " = '" + PropertyValue + "'";
                            }
                            else
                            {
                                setprop += ", " + PropertyName + "= '" + PropertyValue + "'";
                            }
                        }
                        else if (!isFileProperty && PropertyName.ToLower().Contains("file"))
                        {
                            updateFiles(CurrRevision, RelationType, FilesFolder, PropertyValue);
                            
                        }
                    }

                    
                }

                UpdateSQL += " " + setprop + "," + defaultPropSQL + " where id='" + ItemID + "'";
                tech_log.WriteLine("updating the values using update statement -->\nUpdateSQL <--> " + UpdateSQL);
                Item SQLResult = inn.applySQL(UpdateSQL);
                if (SQLResult.isError())
                {
                    Console.WriteLine("Failed to update the record .. " + SQLResult.getErrorString());
                    is_error = true;
                    LineHasError = true;
                    Console.WriteLine("\t\tError while Updating the Properties");
                    log.WriteLine("\t\tError while Updating the Properties");
                    tech_log.WriteLine("\t\tError while Updating the Properties");

                    //throw new Exception();
                }
                else
                {
                       
                    
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while updating the Line" + ex.Message);
                log.WriteLine("Error while updating the Line" + ex.Message);
                tech_log.WriteLine("Error while updating the Line" + ex.Message);
                is_error = true;
                LineHasError = true;
                throw ex;
            }

            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< End the Function .. UpdateExistingRevision >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        }

        private static void UpdateNewItem(Item Result, string ItemType, string RelationType, string FilesFolder, string[] columns, string[] values, out string stateValue, out List<string> filearray, out string setprop)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< Start the Function .. UpdateNewItem >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            stateValue = null;
            filearray = new List<string>();
            try
            {
                //Item CurrRevision = Result.getItemByIndex(0);
                //String ItemID = CurrRevision.getProperty("id");

                //String UpdateSQL = "Update innovator.[" + ItemType + "] set";
                setprop = "";

                tech_log.WriteLine("------------------------");
                tech_log.WriteLine("No of Columns -->" + columns.Length);
                tech_log.WriteLine("No of values  -->" + values.Length);
                tech_log.WriteLine("------------------------");

                for (int inx = 1; inx < columns.Length; inx++)
                {
                    string PropertyName = columns[inx];
                    bool isFileProperty = false;
                    string PropertyValue = getPropertyRealValue(ItemType, columns[inx], values[inx], FilesFolder, out isFileProperty);
                    tech_log.WriteLine("PropertyValue --<" + PropertyValue + "> with length <" + PropertyValue.Length + ">");
                    if (PropertyValue != null && PropertyValue != "" && PropertyValue.Length > 0)
                    {
                        if (PropertyName.ToLower() == "state")
                        {
                            stateValue = PropertyValue;
                        }
                        else if (!PropertyName.ToLower().Contains("file") || (isFileProperty && PropertyName.ToLower().Contains("file")))
                        {
                            Result.setProperty(PropertyName, PropertyValue);
                            if (setprop == "")
                            {
                                setprop = PropertyName + " = '" + PropertyValue + "'";
                            }
                            else
                            {
                                setprop += ", " + PropertyName + "= '" + PropertyValue + "'";
                            }
                        }
                        else if (!isFileProperty && PropertyName.ToLower().Contains("file"))
                        {
                            filearray.Add(PropertyValue);
                            //updateFiles(CurrRevision, RelationType, FilesFolder, PropertyValue);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while updating the Line" + ex.Message);
                log.WriteLine("Error while updating the Line" + ex.Message);
                tech_log.WriteLine("Error while updating the Line" + ex.Message);
                is_error = true;
                LineHasError = true;
                throw ex;
            }

            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< End the Function .. UpdateNewItem >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        }

        private static string getPropertyRealValue(string ItemType, string propertyName, string columnValue, string FilesFolder, out bool isFileProperty)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< Start the Function .. getPropertyRealValue >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            tech_log.WriteLine("propertyName -->"+propertyName);
            tech_log.WriteLine("columnValue -->" + columnValue);
            isFileProperty = false;

            string finalPropertyValue = "";


            string itemId = ItemtypeObject.getID();
            //Item PropertyItem = inn.applySQL("select * from innovator.[Property] where name ='" + propertyName + "' and source_id='" + itemId + "'");
            //string xpathstr = "//Item[@type='Property' and name='" + propertyName + "' and source_id='" + itemId + "']";
            string xpathstr = "//Item[@type='ItemType' and name='" + ItemType + "']/Relationships/Item[@type='Property' and name='" + propertyName + "' and source_id='" + itemId + "']";

            Item PropertyObject = ItemtypeObject.getItemsByXPath(xpathstr);
            if (PropertyObject.getItemCount() > 0 && ! string.IsNullOrEmpty(columnValue))
            {
                string propType = PropertyObject.getItemByIndex(0).getProperty("data_type");
                string propSource = PropertyObject.getItemByIndex(0).getProperty("data_source");

                tech_log.WriteLine("propType -->" + propType);
                tech_log.WriteLine("propSource -->" + propSource);



                if (!string.IsNullOrEmpty(propType))
                {
                    if (propType.ToLower().Contains("item"))
                    {
                        String DataSourceTypeName = PropertyObject.getItemByIndex(0).getPropertyAttribute("data_source", "name");
                        tech_log.WriteLine("DataSourceTypeName -->" + DataSourceTypeName);

                        if (!string.IsNullOrEmpty(DataSourceTypeName) && DataSourceTypeName.ToLower() != "file")
                        {
                            Item datasourceItem = inn.getItemByKeyedName(DataSourceTypeName, columnValue);
                            if (datasourceItem.getItemCount() > 0)
                            {
                                finalPropertyValue = datasourceItem.getItemByIndex(0).getID();
                            }
                        }
                        if (!string.IsNullOrEmpty(DataSourceTypeName) && DataSourceTypeName.ToLower() == "file")
                        {
                            isFileProperty = true;
                            string fileid = createFileObject(columnValue, FilesFolder);

                            finalPropertyValue = fileid;
                        }
                    }
                    else if (propType == "date")
                    {
                        DateTime _date;
                        string day = "";

                        _date = DateTime.ParseExact(columnValue, dateformat, new CultureInfo("en-US"));//("5/13/2012");
                        finalPropertyValue = _date.ToString("yyyy-MM-dd HH':'mm':'ss");

                        //inn.getI18NSessionContext().ConvertToNeutral(utcDt, "date", ""yyyy-MM-dd HH':'mm':'ss"");
                    }
                    else if (propType == "list")
                    {
                        //String listName = PropertyObject.getItemByIndex(0).getPropertyAttribute("data_source", "name");
                       // String listid = PropertyObject.getItemByIndex(0).getPropertyAttribute("data_source", "id");



                        finalPropertyValue = getRealValueFromList(propSource, columnValue); 
                    }
                    else
                    {
                        finalPropertyValue = columnValue;
                    }

                }

                //Console.WriteLine("New Condole");
            }
            else 
            {
                finalPropertyValue = columnValue;
            }
            
            //
            //throw new NotImplementedException();
            tech_log.WriteLine("finalPropertyValue -->"+finalPropertyValue);

            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< End the Function .. getPropertyRealValue >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            return finalPropertyValue;
        }

        private static string getRealValueFromList(string listid, string columnValue)
        {
            string listValue = columnValue;

            String sqlStr = "select * from innovator.[VALUE] where source_id ='" + listid + "' and (VALUE ='" + columnValue + "' or LABEL='" + columnValue + "')";

            Item listVauleItem = inn.applySQL(sqlStr);

            if (!listVauleItem.isError())
            {
                listValue = listVauleItem.getItemByIndex(0).getProperty("value", columnValue);
            }

            return listValue;
        }

        private static string createFileObject(string FileName, string FilesFolder)
        {
            string realFileName = "";
            string filePath = "";
            tech_log.WriteLine("FilesFolder -->" + FilesFolder);
            tech_log.WriteLine("FileName   -->" + FileName);

            if (File.Exists(FileName))
            {
                tech_log.WriteLine("file exist with the path as --> " + FileName);
                realFileName = Path.GetFileName(FileName);
                filePath = Path.GetFullPath(FileName);
            }
            else if (File.Exists(Path.Combine(FilesFolder, FileName)))
            {
                tech_log.WriteLine("file exist with the path as --> " + Path.Combine(FilesFolder, FileName));

                realFileName = Path.GetFileName(Path.Combine(FilesFolder, FileName));
                filePath = Path.GetFullPath(Path.Combine(FilesFolder, FileName));
            }
            else
            {
                is_error = true;
                LineHasError = true;
                tech_log.WriteLine("File not found with the propert value as " + FileName + " in folder Path " + FilesFolder);
                Console.WriteLine("File not found with the propert value as " + FileName + " in folder Path " + FilesFolder);
                //return;
            }

            tech_log.WriteLine("filePath --> " + filePath);
            tech_log.WriteLine("realFileName --> " + realFileName);

            Item fileObj = inn.newItem("File", "add");
            fileObj.setProperty("filename", realFileName);
            fileObj.attachPhysicalFile(filePath);
            fileObj = fileObj.apply();
            if (fileObj.isError())
            {
                
                is_error = true;
                LineHasError = true;
                Console.WriteLine("\t\tError while adding file '" + realFileName + "'.." + fileObj.getErrorString());
                log.WriteLine("\t\tError while adding file '" + realFileName + "'.." + fileObj.getErrorString());
                tech_log.WriteLine("\t\tError while adding file '" + realFileName + "'.." + fileObj.getErrorString());

                //throw new Exception();
            }
            string fileid = fileObj.getID();

            return fileid;
        }

        private static void updateFiles(Item CurrRevision, string RelationType, string FilesFolder, string FileName)
        {
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< Start the Function .. updateFiles >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            string realFileName = "";
            string filePath = "";
            tech_log.WriteLine("FilesFolder -->" + FilesFolder);
            tech_log.WriteLine("FileName   -->" + FileName);

            if (File.Exists(FileName))
            {
                tech_log.WriteLine("file exist with the path as --> " + FileName);
                realFileName = Path.GetFileName(FileName);
                filePath = Path.GetFullPath(FileName);
            }
            else if (File.Exists(Path.Combine(FilesFolder, FileName)))
            {
                tech_log.WriteLine("file exist with the path as --> " + Path.Combine(FilesFolder, FileName));

                realFileName = Path.GetFileName(Path.Combine(FilesFolder, FileName));
                filePath = Path.GetFullPath(Path.Combine(FilesFolder, FileName));
            }
            else
            {
                is_error = true;
                LineHasError = true;
                tech_log.WriteLine("File not found with the propert value as " + FileName + " in folder Path " + FilesFolder);
                Console.WriteLine("File not found with the propert value as " + FileName + " in folder Path " + FilesFolder);
                log.WriteLine("File not found with the propert value as " + FileName + " in folder Path " + FilesFolder);
                return;
            }

            tech_log.WriteLine("filePath --> " + filePath);
            tech_log.WriteLine("realFileName --> " + realFileName);

            Item fileObj = inn.newItem("File", "add");
            fileObj.setProperty("filename", realFileName);
            fileObj.attachPhysicalFile(filePath);
            fileObj = fileObj.apply();
            if (fileObj.isError())
            {
                is_error = true;
                LineHasError = true;
                Console.WriteLine("\t\tError while adding file '" + realFileName + "'.." + fileObj.getErrorString());
                log.WriteLine("\t\tError while adding file '" + realFileName + "'.." + fileObj.getErrorString());
                tech_log.WriteLine("\t\tError while adding file '" + realFileName + "'.." + fileObj.getErrorString());

                //throw new Exception();
            }

            if (fileObj.getItemCount() == 1)
            {

                Item fileRelation = inn.newItem(RelationType, "add");
                fileRelation.setProperty("source_id", CurrRevision.getProperty("id"));
                fileRelation.setPropertyItem("attached_file", fileObj);
                //fileRelation.setRelatedItem(fileObj);
                fileRelation = fileRelation.apply();

                if (fileRelation.isError())
                {
                    is_error = true;
                    LineHasError = true;
                    Console.WriteLine("\t\tError while attaching  file to relationship'" + realFileName + "'.." + fileRelation.getErrorString());
                    log.WriteLine("\t\tError while attaching  file to relationship'" + realFileName + "'.." + fileRelation.getErrorString());
                    tech_log.WriteLine("\t\tError while attaching  file to relationship'" + realFileName + "'.." + fileRelation.getErrorString());


                    //throw new Exception();
                }
            }
            tech_log.WriteLine("<<<<<<<<<<<<<<<<<< End the Function .. updateFiles >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        }

        private static void logout(HttpServerConnection conn)
        {
            conn.Logout(true);
        }

        private static void helpContent()
        {
            Console.WriteLine("#############################################################################################");
            Console.WriteLine("This utility is used to upload the data from txt file into ");
            Console.WriteLine("This utility reads the meta data informaation from config file and the data from import file and import the data to db ");
            Console.WriteLine("*********************************************************************************************");
            Console.WriteLine("below are the parameters utility accept ");
            Console.WriteLine("-user=***  User name to login");
            Console.WriteLine("-p=***  password to login user");
            Console.WriteLine("-url=***  url of the innovator server");
            Console.WriteLine("-db=***  db to which this data has to be imported");
            Console.WriteLine("-input_file=***  complete path of the input file");
            Console.WriteLine("-error_file=***  complete path of the error file 'this is a repeat file to re-import failed Lines'");
            Console.WriteLine("-log_file=***  complete path of the log file");
            Console.WriteLine("-tech_file=***  complete path of the technical log file");
            Console.WriteLine("*********************************************************************************************");
            Console.WriteLine("#############################################################################################");
        }
        static HttpServerConnection login(String url, String db, String user, String password)
        {
            HttpServerConnection conn = IomFactory.CreateHttpServerConnection(url, db, user, password);
            tech_log.WriteLine("Logging with user " + user + ".....");

            Item login_result = conn.Login();
            if (login_result.isError())
            {
                //return null;
                tech_log.WriteLine("Login failed for user " + user + "..... \n" + login_result.getErrorString());
                Console.WriteLine("Login failed for user " + user + "..... \n" + login_result.getErrorString());
                return null;
                throw new Exception("Login failed" + login_result.getErrorString());

            }
            else
            {
                tech_log.WriteLine("Login sucessful for user " + user + ".....");

            }

            return conn;
        }
    }
}
