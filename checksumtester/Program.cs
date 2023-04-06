using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Odbc;
using System.IO;
using Microsoft.Win32;
//using System.Collections;
//using System.IO.Compression;
//using System.Runtime.Remoting.Messaging;
//using static checksumtester.Program;
//using System.Xml.Linq;
//using System.IO.Pipes;
//using System.Reflection;
//using System.Threading;

namespace checksumtester
{
    public class ChkPair
    {
        private readonly long size_;
        private readonly long chks_;
        private readonly long index;
        private readonly string _guid;
        private readonly string _blockGuid;
        public long Chks { get => chks_; }
        public long Index { get => index; }

        public string guid => _guid;

        public string blockGuid => _blockGuid;

        public long Size => size_;

        public ChkPair(long chks_, long index, long size_)
        {
            this.chks_ = chks_;
            this.index = index;
            this._guid = Guid.NewGuid().ToString();
            this._blockGuid = Guid.NewGuid().ToString();
            this.size_ = size_;
        }

        public ChkPair(long chks_, long index, string guid_, long size_)
        {
            this.chks_ = chks_;
            this.index = index;
            this._guid = guid_;
            this.size_ = size_;
        }

        public ChkPair(long chks_, long index, string guid_, string blockGuid, long size_)
        {
            this.chks_ = chks_;
            this.index = index;
            this._guid = guid_;
            this._blockGuid = blockGuid;
            this.size_ = size_;
        }


    }


    public class Odbc_
    {

        private OdbcConnection cnn;
        public static int objectId;
        public
        Odbc_(string connetionString)
        {
            objectId = 0;
            cnn = new OdbcConnection(connetionString);
        }


        public
       void
       ExecuteCommand(string command)
        {

            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                var test = cmd.ExecuteNonQuery();
                cnn.Close();
            }
        }

        public
        static
        string
        GetDbName()
        {
            var key = Registry.CurrentUser.OpenSubKey("SOFTWARE\\fileArc");
            return (key.GetValue("DbName").ToString());
        }

        public
        static
        string
        GetSqlMachineName()
        //TODO the name machine of sql database!!!!
        //  
        //
        {
            var key = Registry.CurrentUser.OpenSubKey("SOFTWARE\\fileArc");
            return (key.GetValue("sqlServerMachineName").ToString());
        }

        /*
         * use archiver SELECT FA.checkCode,FA.idx,FA.GUID,FA.blockGUID,FA.size from  FILEATTR FA where blockGUID = '9D950A20-6836-428B-AB7B-5810876B8F64'
         * 
         * */
        public
        List<string>
        ExecuteStringsQuery(string command)
        {
            List<string> ret = new List<string>();
            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ret.Add(reader.GetString(0));
                    }
                    cnn.Close();
                    return (ret);
                }
            }
        }

        public
        KeyValuePair<long, string>
        ExecuteDualQueryLS(string command)
        {

            KeyValuePair<long, string> ret = new KeyValuePair<long, string>(-1, "");
            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var t = (long)reader.GetDecimal(0);
                        var n = reader.GetGuid(1).ToString();
                        KeyValuePair<long, string> keyValuePair = new KeyValuePair<long, string>((long)reader.GetDecimal(0), reader.GetGuid(1).ToString());
                        cnn.Close();
                        return (keyValuePair);
                      //  keyValuePair.Value = reader.GetString(0);
                      //  keyValuePair.Key = reader.GetString(1);
              //          ret.Add(reader.GetString(0));
                    }
                    cnn.Close();
                    return (ret);
                }
            }
        }


        public
        KeyValuePair<long, string>
        ExecuteDualQuerySL(string command)
        {

            KeyValuePair<long, string> ret = new KeyValuePair<long, string>(-1, "");
            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var t = (long)reader.GetDecimal(1);
                        var n = reader.GetGuid(0).ToString();
                        KeyValuePair<long, string> keyValuePair = new KeyValuePair<long, string>((long)reader.GetDecimal(1), reader.GetGuid(0).ToString());
                        cnn.Close();
                        return (keyValuePair);
                        //  keyValuePair.Value = reader.GetString(0);
                        //  keyValuePair.Key = reader.GetString(1);
                        //          ret.Add(reader.GetString(0));
                    }
                    cnn.Close();
                    return (ret);
                }
            }
        }






        public
        string[]
      ExecuteTupleStrings(string command)
        {
            string[] ret = new string[2];
            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ret[0] = reader.GetString(0);
                        ret[1] = reader.GetString(1);
                        // reader.GetDecimal(0);
                    }
                    cnn.Close();
                    return (ret);
                }
            }
        }

        public
        string
        ExecuteStringQuery(string command)
        {
            string ret = null;
            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ret = reader.GetString(0);
                        // reader.GetDecimal(0);
                    }
                    cnn.Close();
                    return (ret);
                }
            }
        }

        public
       // List<ChkPair>
        Dictionary<long, ChkPair>
        ExecutechkPairsQuery(string command)
        {

            Dictionary<long, ChkPair> ret = new Dictionary<long, ChkPair>();

            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ChkPair cp = new ChkPair(((long)reader.GetDecimal(0)), ((long)reader.GetDecimal(1)), reader.GetString(2), reader.GetString(3), (long)reader.GetDecimal(4));
                        ret.Add(cp.Chks, cp);
                    }
                    cnn.Close();
                    return (ret);
                }
            }


        }

        public
        long
        ExecuteIntQuery(string command)
        {
            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        long ret = reader.GetInt32(0);
                        cnn.Close();
                        return (ret);

                    }
                    else
                    {
                        cnn.Close();
                    }
                }
            }
            return (0);
        }


        public
        bool
        Table_checked(string tablename) =>
           ExecuteIntQuery("use " + Odbc_.GetDbName() + " IF (OBJECT_ID('" + tablename + "', 'U') IS NOT NULL) SELECT 1 AS RET ELSE SELECT 0 AS RET") == 1;


    }

    public class FileControl
    {

        private bool _error;
        private long blockSize_;
        private long remainder;
        private long position;
        BinaryReader reader;
        private readonly long fileSize;
        private byte[] rawData;
        private readonly string filename;
        private long count;

        public FileControl(string file_, int blockCount)
        {
            _error = false;
            count = 0;
            this.filename = file_;
            try
            {
                FileStream fileStream = new FileStream(file_, FileMode.Open, FileAccess.Read);
                fileSize = fileStream.Length;
                position = 0;
                fileStream.Close();
            }
            catch (IOException e)
            {
                _error = true;
                Console.BackgroundColor = ConsoleColor.Red;
                Console.WriteLine(e.Message);
                //throw new Exception("Exception!");
            }
            //long test = FileSize / blockSize;
            if (!errorStatus)
            {
                reader = new BinaryReader(File.Open(file_, FileMode.Open, FileAccess.Read));
                blockSize_ = RecomendedBlock(blockCount);
            }

            RawData = new byte[blockSize_];
        }



        public
        bool First()
        {
           // get
           // {
                if ((errorStatus) || (Reader.BaseStream == null))
                    return (false);
                Reader.BaseStream.Seek(position, SeekOrigin.Begin);
                long reminder = FileSize - position;
                if ((reminder < blockSize_) && (reminder > 0))
                    blockSize_ = reminder;
                RawData = Reader.ReadBytes((int)blockSize_);
                position += RawData.Length;
                return (position <= FileSize);
           // }
        }

        public
        bool Next()
        {
           // get
           // {
                if (errorStatus)
                    return (false);

                if (position == FileSize)
                    return false;
                Reader.BaseStream.Seek(position, SeekOrigin.Begin);
                long reminder = FileSize - position;
                if ((reminder < blockSize_) && (reminder > 0))
                    blockSize_ = reminder;
                RawData = Reader.ReadBytes((int)blockSize_);
                position += RawData.Length;
                count += 1;
                return (position <= FileSize);
           // }
        }

        public
        void
        end()
        {
            if (reader != null)
                reader.Close();


        }

        public
        long
        RecomendedBlock(int wantedBlocksCount)
        {
            if (fileSize > 100000000)
            {
                remainder = fileSize % wantedBlocksCount;
                return (fileSize / wantedBlocksCount);
            }
            else
                return (fileSize);
        }



        public byte[] RawData { get => rawData; set => rawData = value; }
        public long FileSize { get => fileSize; }
        public long BlockSize { get => blockSize_; }
        public long Count { get => count; }
        public string Filename { get => filename; }
        public long Remainder { get => remainder; }
        public BinaryReader Reader { get => reader; } // set => reader = value; }
        public bool errorStatus { get => _error; }
        //  public long BlockRead { get => blockRead; set => blockRead = value; }
    }





    public class ChecksumController
    {
        private readonly long chkValue;
        long size_;
        public
        ChecksumController(byte[] data)
        {
            size_ = data.Length;
            chkValue = ChkSum(data);
        }


        public long Size { get => size_; }

        public long ChkValue => chkValue;

        private
        long ChkSum(byte[] rawData)
        {
            long result_ = 0;
            for (long i = 0, j = rawData.Length - 1; true; ++i, --j)
            {
                if (j <= i)
                    break;
                long part_i = (rawData[i] * ((i == 0) ? 1 : i));
                long part_ii = ((rawData[j] * j) == 0) ? 1 : rawData[j];
                result_ += (part_i ^ part_ii);
            }
            return (result_);
        }

        static
        public
        string
        GetSqlMachineName()
        {
            var key = Registry.CurrentUser.OpenSubKey("SOFTWARE\\RMP_GIS");
            return (key.GetValue("sqlServerMachineName").ToString());
        }

    }


    internal class Program
    {

        public
        static
        void
        generateBlocksByState(string state, ref Odbc_ odbc, ChkPair value_)
        {
            //value_.blockGuid
            //File.CreateDirector

            Directory.CreateDirectory("D:\\out\\" + state);

            var pair =
            odbc.ExecuteTupleStrings("SELECT FO.Name,DO.Name from  dirobj DO inner join FILEOBJ FO on FO.dirGUID = DO.dirGUID inner join FILEATTR FA on FA.GUID = FO.GUID where FA.GUID = '" + value_.guid.ToString() + "' and blockGUID='" + value_.blockGuid.ToString() + "'");
            string fullfileName = pair[1] + pair[0];
            FileControl fc = new FileControl(fullfileName, 1000);
            long count_ = 0;
            if (fc.errorStatus)
            {
                fc.end();
                return;
            }
            if (fc.First())
            {
                do
                {
                    //        ChecksumController cc = new ChecksumController(fc.RawData);
                    //  ChkPair cp = new ChkPair(cc.ChkValue, fc.Count, fc.RawData.Length);

                    //   Directory.CreateDirectory("d:\\out\\" + state);
                    string fileOut = "d:\\out\\" + state + "\\" + value_.blockGuid;
                    if (value_.Index == count_)
                    {
                        ChecksumController cc = new ChecksumController(fc.RawData);
                        // odbc.ExecuteCommand("use " + Odbc_.GetDbName() +
                        //     " UPDATE FILEATTR SET stateGuid ='" + state + "' where GUID ='" + value_.guid + "' and idx = " + value_.Index.ToString() + " and blockGUID='" + value_.blockGuid + "'");
                        //   odbc.ExecuteStringQuery("use " + Odbc_.GetDbName() + "")

                        odbc.ExecuteCommand("use " + Odbc_.GetDbName() +
                            " insert into FILEATTR(stateGuid,GUID,blockGUID,checkCode,idx,size) VALUES('" + state + "','" + value_.guid + "','" + value_.blockGuid + "'," + cc.ChkValue.ToString() + "," + value_.Index.ToString() + "," + cc.Size.ToString() + ")");

                        File.WriteAllBytes(fileOut, fc.RawData);
                    }
                    ++count_;
                }
                while (fc.Next());
                fc.end();
            }
        }

        public
        static
        void
        Generate_blocks(string state, ref Odbc_ odbc, string fullFileName)
        {

            // string str = "Driver={SQL Server};";
            // str += @"Server=" + odbc_.getSqlMachineName() + "\\SQLEXPRESS";
            // str += ";DataBase=" + odbc_.getDbName() + ";Trusted_Connection=Yes;";
            // odbc_ odbc = new odbc_(str);

            string fileGuid = Guid.NewGuid().ToString();
            string dirGuid = Guid.NewGuid().ToString();
            string filename = System.IO.Path.GetFileName(fullFileName);
            string filePath = fullFileName.Remove(fullFileName.IndexOf(filename), filename.Length);
            List<string> lstDirObj = odbc.ExecuteStringsQuery("use " + Odbc_.GetDbName() + " select dirGUID from DIROBJ where Name like '" + filePath + "'");
            if (lstDirObj.Count < 1)
                odbc.ExecuteCommand("use " + Odbc_.GetDbName() + " insert into DIROBJ(Name,dirGUID) VALUES('" + filePath + "','" + dirGuid + "')");

            FileControl fc = new FileControl(fullFileName, 1000);
            if (fc.errorStatus)
            {
                fc.end();
                return;
            }
            filename = filename.Replace("'", "''");
            odbc.ExecuteCommand("use " + Odbc_.GetDbName() + " insert into FILEOBJ(Name,GUID,dirGUID,size,blockSize) VALUES('" + filename + "','" + fileGuid
            + "','" + ((lstDirObj.Count > 0) ? lstDirObj[0] : dirGuid) + "'," + fc.FileSize + "," + fc.BlockSize + ")");

            List<ChkPair> blocks = new List<ChkPair>();
            //HashSet<chkPair> blocks = new HashSet<chkPair>();

            long count = 0;
            if (fc.errorStatus)
            {
                fc.end();
                return;
            }
            if (fc.First())
            {
                do
                {
                    ChecksumController cc = new ChecksumController(fc.RawData);
                    ChkPair cp = new ChkPair(cc.ChkValue, fc.Count, fc.RawData.Length);
                    //           blocks.Add(cp);
                    odbc.ExecuteCommand("use " + Odbc_.GetDbName() +
                        " insert into FILEATTR(stateGuid,GUID,checkCode,idx,blockGUID,size) VALUES('" + state + "','" + fileGuid + "'," + cp.Chks.ToString() + "," + cp.Index.ToString() + ",'" + cp.blockGuid + "'," + cp.Size.ToString() + ")");

                    Directory.CreateDirectory("d:\\out\\" + state);
                    string fileOut = "d:\\out\\" + state + "\\" + cp.blockGuid;
                    File.WriteAllBytes(fileOut, fc.RawData);
                    ++count;
                }
                while (fc.Next());
            }
            fc.end();
            // for (int i = 0; i < blocks.Count; ++i)

            return;
        }

        /* public
         static
         bool
         UpdateBlock(ref Odbc_ odbc, ChkPair pair, byte[] rawData)
         {
             string guid_ = Guid.NewGuid().ToString();
             Console.WriteLine(pair.guid);
             odbc.ExecuteCommand("use " + Odbc_.GetDbName() +
                 " UPDATE FILEATTR SET blockGUID ='" + guid_ + "' where GUID ='" + pair.guid + "' and idx = " + pair.Index.ToString());
             odbc.ExecuteCommand("use " + Odbc_.GetDbName() +
                 " insert into FILEATTR(GUID,checkCode,idx) VALUES('" + guid_ + "'," + pair.Chks.ToString() + "," + pair.Index.ToString() + ")");

             //  File.WriteAllBytes(guid_, rawData);
             return true;

         }*/

        public
        static
        Odbc_ Init()
        {
            string str = "Driver={SQL Server};";
            str += @"Server=" + Odbc_.GetSqlMachineName() + "\\SQLEXPRESS" +
                    ";DataBase=" + Odbc_.GetDbName() + ";Trusted_Connection=Yes;";
            Odbc_ odbc = new Odbc_(str);
            Dictionary<string, string> dictOfQuery = new Dictionary<string, string>();
            dictOfQuery["FILEOBJ"] = "use " + Odbc_.GetDbName() + " CREATE TABLE FILEOBJ(Name varchar(260),GUID UNIQUEIDENTIFIER,dirGUID UNIQUEIDENTIFIER,size BIGINT,blockSize BIGINT)";
            dictOfQuery["FILEATTR"] = "use " + Odbc_.GetDbName() + " CREATE TABLE FILEATTR(stateGuid UNIQUEIDENTIFIER, GUID UNIQUEIDENTIFIER ,checkCode BIGINT,idx BIGINT,blockGUID UNIQUEIDENTIFIER,size BIGINT)";
            dictOfQuery["DIROBJ"] = "use " + Odbc_.GetDbName() + " CREATE TABLE DIROBJ(Name varchar(260),dirGUID UNIQUEIDENTIFIER)";
            dictOfQuery["STATECOLLECTOR"] = "use " + Odbc_.GetDbName() + " CREATE TABLE STATECOLLECTOR(Id int IDENTITY(1,1) PRIMARY KEY,stateGUID UNIQUEIDENTIFIER,_DATE DATETIME)";

            //Хер его знает зачем, пока сам не понял зачем мне эта таблица
// dictOfQuery["FILESTATES"] = "use " + Odbc_.GetDbName() + " CREATE table FileStates(fileGUID uniqueidentifier,stateGuid uniqueidentifier)";
            for (int i = 0; i < dictOfQuery.Count; ++i)
            {
                if (!odbc.Table_checked(dictOfQuery.ElementAt(i).Key))
                    odbc.ExecuteCommand(dictOfQuery.ElementAt(i).Value);
            }
          //  if (getLayersCount(ref odbc) < 1)
                createDirCheckInfo(ref odbc, "D:\\ttt\\");
            return (odbc);
        }

        public
        static
        string[]
        FindDirectories(string path)
        {
            string[] dir;
            try
            {
                dir = Directory.GetDirectories(path);
            }
            catch (Exception)
            {
                return (null);
            }
            return dir;
        }

        public
        static
        string[]
        Findfiles(string path)
        {
            string[] files;
            files = Directory.GetFiles(path);
            var len_ = files.Length;
            return (files);
        }

        public
        static
        bool
        CopyTo(string zipFilePatnAndName, string fileName)
        {

            FileStream zipToOpen = new FileStream(zipFilePatnAndName, FileMode.Create);
            System.IO.Compression.ZipArchive archive = new System.IO.Compression.ZipArchive(zipToOpen, System.IO.Compression.ZipArchiveMode.Create);
            System.IO.Compression.ZipArchiveEntry _entry = archive.CreateEntry(System.IO.Path.GetFileName(fileName));
            FileControl fc = new FileControl(fileName, 40);
            using (BinaryWriter writer = new BinaryWriter(_entry.Open()))
            {
                while (fc.Next())
                {
                    writer.Write(fc.RawData);
                }
                fc.end();
                // using (StreamWriter writer = new StreamWriter(_entry.Open()))
                // {
                //_list.ForEach(s => writer.WriteLine(s));
                // writer.Write()
                writer.Close();
            }

            zipToOpen.Close();
            return (true);
        }

        static
        string[]
        FullDirController(string targetDir, ref List<string> listOfDirectory)
        {
            listOfDirectory.Add(targetDir);
            var dirs = FindDirectories(targetDir);
            
            if (dirs == null)
                return null;
            if (dirs.Length == 0)
                return null;


            

            for (int index = 0; index < dirs.Length; index++)
            {
                string _tdir = dirs[index];
                var dirs_ = FullDirController(_tdir, ref listOfDirectory);
                if (dirs_ == null)
                    continue;

                return (dirs_);
            }
            return null;

        }


        public
        static
        void
        createDirCheckInfo(ref Odbc_ odbc, string inputDir)
        {
            //  odbc.ExecuteIntQuery
            //SELECT count(*) as count  FROM[archiver].[dbo].STATECOLLECTOR
            string state = Guid.NewGuid().ToString();
            var test = odbc.ExecuteIntQuery("use " + Odbc_.GetDbName() + " SELECT count(*) from STATECOLLECTOR");
            if (test == 0)
            {
           //     state = odbc.ExecuteStringQuery("use " + Odbc_.GetDbName() + " select stateGUID from STATECOLLECTOR where Id = 1");
                odbc.ExecuteCommand("use " + Odbc_.GetDbName() + " insert into STATECOLLECTOR(stateGUID,_DATE) VALUES('" + state + "',GETDATE())");
            }
           //     odbc.ExecuteCommand("use " + Odbc_.GetDbName() + "select MAX(Id) as max_id from STATECOLLECTOR")



            List<string> listOfDirectory = new List<string>();
            FullDirController(inputDir, ref listOfDirectory);
            foreach (var dir in listOfDirectory)
            {
                if (dir.IndexOf("Office")>-1)
                {
                    Console.WriteLine("test.");
                }
                var str = Findfiles(dir);
                for (int j = 0; j < str.Length; ++j)
                {

                    Console.WriteLine(str[j]);
                    string dir_ = System.IO.Path.GetDirectoryName(str[j])+"\\";
                    string filename = str[j].Replace(dir_, "");
                    filename = filename.Replace("'", "''");
                    if (filename.IndexOf("XmlViewer.exe")>-1 )
                    {
                        Console.WriteLine("test.");
                    }
                    test =  odbc.ExecuteIntQuery("use " + Odbc_.GetDbName() + " SELECT count(*) from FILEOBJ where Name = '"+ filename + "'");
                    ////////////////////////////
                    //Допилить проверку на наличие новых файлов и блокогенерацию.
                    //
                    //
                    //


                    if (test == 0)
                    {
                        state = odbc.ExecuteStringQuery("use " + Odbc_.GetDbName() + " select stateGUID from STATECOLLECTOR where Id = 1");
                        Generate_blocks(state, ref odbc, str[j]);
                    }
                }
            }
        }




        private
        static
        Dictionary<long, ChkPair>
        getBlocks(KeyValuePair<long, ChkPair>[] test, ref FileControl fc)
        {
           // HashSet<KeyValuePair<long, ChkPair>> foundedPairs = new HashSet<KeyValuePair<long, ChkPair>>();
            Dictionary<long, ChkPair> founedObject = new Dictionary<long, ChkPair>();
            fc.First();
            do
            {
                int index = (int)fc.Count;
                if ((index < 0) || (index > test.Length ))
                    continue;

                ChecksumController cc = new ChecksumController(fc.RawData);
                ChkPair cp = new ChkPair(cc.ChkValue, index, test[index].Value.guid, test[index].Value.blockGuid, cc.Size);
                founedObject[cc.ChkValue] = cp;

            } while (fc.Next());
            fc.end();
            return (founedObject);
        }

        public
        static
        Dictionary<long, ChkPair>
        getCheckedPairs(ref Odbc_ odbc, string fullFileName, string state)
        {
            string _filename = System.IO.Path.GetFileName(fullFileName);
            string _filePath = fullFileName.Remove(fullFileName.IndexOf(_filename), _filename.Length);
            _filename = _filename.Replace("'", "''");
            string query = "use " + Odbc_.GetDbName() + " SELECT FA.checkCode,FA.idx,FA.GUID,FA.blockGUID,FA.size from  dirobj DO inner join FILEOBJ FO on FO.dirGUID = DO.dirGUID inner join FILEATTR FA on FA.GUID = FO.GUID";
            query += "  where DO.Name like '" + _filePath + "' and FO.Name = '" + _filename + "' and FA.stateGuid='" + state + "'order by idx";
            return (odbc.ExecutechkPairsQuery(query));
        }

        private
        static
        List<string>
        getBlocksGuids(ref Odbc_ odbc)
        {
            return (odbc.ExecuteStringsQuery("SELECT distinct blockGUID FROM FILEATTR"));
        }

        private
        static
        List<string>
        getStateHistory(ref Odbc_ odbc, ChkPair value)
        {
            var list = odbc.ExecuteStringsQuery("use " + Odbc_.GetDbName() +
                " SELECT FA.stateGUID from  FILEATTR FA where blockGUID = '" + value.blockGuid.ToString()+ "' and checkCode = "+value.Chks.ToString());
            // and checkCode="+value.Chks);
            return (list);
        }

 /*       private
        static
        List<string>
        chkchk(ref Odbc_ odbc, ChkPair value)
        {
            var list = odbc.ExecuteStringsQuery("use " + Odbc_.GetDbName() +
                " SELECT FA.stateGUID from  FILEATTR FA where checkCode = " + value.Chks.ToString());
            return (list);
        }

        */
        /* private
         static
         long
         (ref Odbc_ odbc)
         {
             return (odbc.ExecuteIntQuery( "use " + Odbc_.GetDbName() + " select count(*) as COUNT_ from STATECOLLECTOR"));
         }*/


        private
        static
        KeyValuePair<long, string>
        getCheckPairByBlockId(ref Odbc_ odbc,string blockGuid)
        {
           string query = "use " + Odbc_.GetDbName() +
                " SELECT FA.stateGuid , ST.Id FROM FILEATTR FA inner join STATECOLLECTOR ST on ST.stateGUID = FA.stateGuid where blockGUID = '" + blockGuid + "'";//
           var x = odbc.ExecuteDualQuerySL(query);
           return x ;
        }

       /* private
        static
        long
        getLayersList(ref Odbc_ odbc)
        {
            return (odbc.ExecuteIntQuery("use " + Odbc_.GetDbName() + " SELECT  FROM STATECOLLECTOR"));
        }*/

        private 
        static
        long
        getLayersCount(ref Odbc_ odbc)
        {
            return (odbc.ExecuteIntQuery("use " + Odbc_.GetDbName() + " SELECT count(*) FROM STATECOLLECTOR"));   
        }


        public 
        static 
        KeyValuePair<long,string>
        getNewStateGuidsFor(ref Odbc_ odbc,string blockGuid)
        {
            string q = " declare @T table(Id INT, stateGUID uniqueidentifier)";
            q += " insert into @T(Id, stateGUID) select SC.Id as Id , SC.stateGUID as stateGUID from FILEATTR FA";
            q += " inner join STATECOLLECTOR SC on SC.stateGUID = FA.stateGuid  where FA.blockGUID = '" + blockGuid + "'";
            q+= " select SC.Id as s_Id , SC.stateGUID as scState  from STATECOLLECTOR SC left join @T T on T.Id = SC.Id";
            q += " where T.Id is null ";
            return (odbc.ExecuteDualQueryLS(q));
        }
        
        public
        static
        List<string>
        getBlocksGuidsByCheckCode(ref Odbc_ odbc, ChkPair pair)
        {
            return (odbc.ExecuteStringsQuery("select blockGuid from FILEATTR where checkCode =" + pair.Chks.ToString()));

        }

        private
        static
        Dictionary<long,ChkPair>
        getDifferents(ref Odbc_ odbc,string fullFileName,string state_)
        {
            if (fullFileName == "D:\\ttt\\Hronoki.lombarda.2013.L1.HDRip.2100MB.avi")
            {
                Console.WriteLine("test");
            }
            FileControl fc = new FileControl(fullFileName, 1000);
            if (fc.errorStatus)
            if (!fc.First())
            {
                fc.end();
            }
            var chekedPairs = getCheckedPairs(ref odbc, fullFileName, state_);
            Dictionary<long, ChkPair> checkedChangedBlocks = new Dictionary<long, ChkPair>();
            if (chekedPairs.Count() == 0)
                return null;
            Dictionary<long, ChkPair> founedObject = getBlocks(chekedPairs.ToArray(), ref fc);
            foreach (var f in founedObject)
            {
                if (!chekedPairs.ContainsKey(f.Value.Chks))
                {
                    checkedChangedBlocks.Add(f.Value.Chks, f.Value);
                }
            }
            fc.end();
            return checkedChangedBlocks;    
        }




        public
        static
       // HashSet<KeyValuePair<long, ChkPair>>
        Dictionary<long,ChkPair>
        CheckFiles(ref Odbc_ odbc, string dir)
        {
            // HashSet<KeyValuePair<long, ChkPair>> foundedPairs = new HashSet<KeyValuePair<long, ChkPair>>();
            Dictionary <long,ChkPair> checkedChangedBlocks = new Dictionary<long, ChkPair> ();
            var fullFileNames = Findfiles(dir);//findfiles("D:\\garbage\\","D:\\garbage\\arc2022.zip");
            if (0 == fullFileNames.Length)
            {

            }
            var ListOfStates = odbc.ExecuteStringsQuery("use " + Odbc_.GetDbName() + " SELECT stateGUID FROM STATECOLLECTOR  order by id asc");
            if (ListOfStates.Count == 0)
                return null;
            long layersCount = getLayersCount(ref odbc);
            foreach (string fullFileName in fullFileNames)
            {
                Dictionary<long, ChkPair> checkedPairs = new Dictionary<long, ChkPair>();
                int i = 0;
                Console.WriteLine("checked:" + fullFileName);
                Dictionary<long, ChkPair> detected_ = new Dictionary<long, ChkPair>();
                var _state = ListOfStates.ElementAt(0);

                do
                {
                    var chekedPairs = getCheckedPairs(ref odbc, fullFileName, _state);

                    int counter_ = 0;

                    Dictionary<long, ChkPair> _checkedPairs = getDifferents(ref odbc, fullFileName,_state );
                    if (_checkedPairs == null)
                        break;
                    foreach (var v in _checkedPairs)
                    {
                       List<string> lst = getStateHistory(ref odbc, v.Value);
                       if (lst.Count > 0)
                        {
                            Console.WriteLine("test(++)");
                            Console.WriteLine("Index of element = " + v.Value.Index.ToString());
                            foreach (var k in lst)
                                Console.WriteLine(k);
                            Console.WriteLine("test(--)");
                            continue;                            
                        }
                        if (!checkedChangedBlocks.ContainsKey(v.Key))
                            checkedChangedBlocks.Add(v.Key,v.Value);

                    }
                    ++i;
                }
                while (i < ListOfStates.Count);
            }
           
            return checkedChangedBlocks;
        }
           
        //}



        public static
         HashSet<KeyValuePair<long, ChkPair>>
        CheckController(ref Odbc_ odbc, string inputDir)
        {
           // bool ret = false;
            HashSet<KeyValuePair<long, ChkPair>> hs = new HashSet<KeyValuePair<long, ChkPair>>();
            List<string> listOfDirectory = new List<string>();
            FullDirController(inputDir, ref listOfDirectory);
            if (listOfDirectory.Count > 0)
                foreach (var dir in listOfDirectory)
                {
                    if (dir.IndexOf("Office") >-1)
                    {
                        Console.WriteLine("test.");
                    }
                    var files = CheckFiles(ref odbc, dir);
                    if ((files == null) || (files.Count == 0))
                    {
                       // FullDirController(dir, ref listOfDirectory);
                        continue;
                    }
                    foreach (var v in files)
                        hs.Add(v);
                }
            else
            {
                var files = CheckFiles(ref odbc, inputDir);
                if ((files == null) || (files.Count == 0))
                    return null;

                foreach (var v in CheckFiles(ref odbc, inputDir))
                    hs.Add(v);
            }
            return (hs);
        }

        //     }

        public static void createOrAppendAllBytes(string path, byte[] bytes)
        {
            //argument-checking here.
            if (File.Exists(path))
            {
                using (var stream = new FileStream(path, FileMode.Append))
                {
                    stream.Write(bytes, 0, bytes.Length);
                }
            }
            else
            {
                using (var stream = new FileStream(path, FileMode.Create))
                {
                    stream.Write(bytes, 0, bytes.Length);
                }
            }
        }


        static void Main(string[] args)
        {
            Odbc_ _odbc = Init();

 
            var objectsDetected = CheckController(ref _odbc, "D:\\ttt\\");
            if ((objectsDetected != null) && (objectsDetected.Count != 0)) 
            {
                // TODO: Придумать событие, которое будет приводить к созданию нового слоя.
                // в данном случае новый слой создаётся всегда.
                //
                
                foreach (var v in objectsDetected)
                {
                    var control = getCheckPairByBlockId(ref _odbc, v.Value.blockGuid);
                    var key =  control.Key;
                    var State = getNewStateGuidsFor(ref _odbc, v.Value.blockGuid);
                    if (State.Key == -1)
                    {
                        string state_ = Guid.NewGuid().ToString();
                        _odbc.ExecuteCommand("use " + Odbc_.GetDbName() + " insert into STATECOLLECTOR(stateGUID,_DATE) VALUES('" + state_ + "',GETDATE())");
                        State = getNewStateGuidsFor(ref _odbc, v.Value.blockGuid);
                    }

                    Console.WriteLine(v.Key.ToString() +":" +v.Value.guid);
                    generateBlocksByState(State.Value, ref _odbc, v.Value);
                }
            }
        }
    }
}
        
