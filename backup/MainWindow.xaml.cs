using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Microsoft.Win32;
using System.Data.Odbc;

namespace backup
{
    /// <summary>
    /// Логика взаимодействия для MainWindow.xaml
    /// </summary>
    /// 
    public class Odbc_
    {
       public struct _u_val
        {
            public _u_val(Int32 _val, string _value)
            {
                this._val = _val;
                this._value = _value;
            }

            _u_val(Int32 _val)
            {
                this._val = _val;
                this._value = "NONE";
            }

            _u_val(string _value)
            {
                this._val = -1;
                this._value = _value;
            }

 
            Int32 _val;
          //  Int32 _val_next;
            string _value;
        }


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
         * use archiver 
         *  SC.Id,FO.Name, FA.stateGuid,idx,checkCode
         * */
        public
         List<Tuple<Int32, string, string, string, string, Int32, Decimal>>
        ExecuteStringsQuery(string command)
        {
            //SC.Id,DO.Name,FO.Name, DO.Name + FO.Name as FullPath, FA.stateGuid,idx,checkCode
            List<Tuple<Int32, string, string, string, string, Int32, Decimal>> ret = new List<Tuple<Int32, string, string, string, string, Int32, Decimal>>();
            using (OdbcCommand cmd = new OdbcCommand(command, cnn))
            {
                cnn.Open();
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Tuple<Int32, string, string, string, string, Int32, Decimal> _Tuple = 
                            new Tuple<Int32, string, string, string, string, Int32, Decimal>(reader.GetInt32(0), reader.GetString(1), reader.GetString(2) , reader.GetString(3), reader.GetString(4),reader.GetInt32(5), reader.GetDecimal(6));
                        ret.Add(_Tuple);
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
    }

    public partial class MainWindow : Window
    {

        public
        static
        Odbc_ Init()
        {
            string str = "Driver={SQL Server};";
            str += @"Server=" + Odbc_.GetSqlMachineName() + "\\SQLEXPRESS" +
                    ";DataBase=" + Odbc_.GetDbName() + ";Trusted_Connection=Yes;";
            Odbc_ odbc = new Odbc_(str);
            return (odbc);
        }

            public MainWindow()
        {
            InitializeComponent();
            Odbc_ odbc = Init();

            string query = "use " + Odbc_.GetDbName() + " select SC.Id,DO.Name,FO.Name, DO.Name + FO.Name as FullPath, FA.stateGuid,idx,checkCode from FILEATTR FA inner join FILEOBJ FO on FO.GUID = FA.GUID inner join STATECOLLECTOR SC on SC.stateGUID = FA.stateGuid inner join DIROBJ DO on DO.dirGUID = FO.dirGUID";
            query += " where SC.Id > 1 order by SC.Id";
            var out_ = odbc.ExecuteStringsQuery(query);
            Dictionary<string , Int32> checkedFiles =  new Dictionary<string , Int32>();
            //            HashSet<KeyValuePair<long, string>> foundedPairs = new HashSet<KeyValuePair<long, string>>(); ;
            Dictionary< Int32, List<Tuple<string,string>> > checkedFilesList = new Dictionary< Int32 , List<Tuple<string, string>> >();
            foreach (var v in out_)
            {
                if (!checkedFiles.ContainsKey(v.Item2))
                {
                    if (!checkedFilesList.ContainsKey(v.Item1))
                    {
                        checkedFilesList[v.Item1] = new List<Tuple<string,string>>();
                        checkedFilesList[v.Item1].Add(new Tuple<string,string>(v.Item2,v.Item3));
                    }
                    else if (!checkedFilesList[v.Item1].Contains(new Tuple<string, string>(v.Item2,v.Item3)))
                        checkedFilesList[v.Item1].Add(new Tuple<string, string>(v.Item2, v.Item3));
                    
                    checkedFiles[v.Item2] = v.Item1;
                }                
                else
                {
                    //foundedPairs.Add(new KeyValuePair<long, string>(v.Item1, v.Item2));
                    if (!checkedFilesList.ContainsKey(v.Item1))
                    {
                        checkedFilesList[v.Item1] = new List<Tuple<string, string>>();
                        checkedFilesList[v.Item1].Add(new Tuple<string, string>(v.Item2, v.Item3));

                    }
                    else
                    {
                       if (!checkedFilesList[v.Item1].Contains(new Tuple<string, string>(v.Item2, v.Item3)))
                         checkedFilesList[v.Item1].Add(new Tuple<string, string>(v.Item2, v.Item3));
                    }
                }
            }
            TreeViewItem ParentItem = new TreeViewItem();
            ParentItem.Header = "test";
            TreeView1.Items.Add(ParentItem);
            //TreeViewItem KeyOfFileNameObjects = null;
            var keys_ = checkedFilesList.Keys;
            foreach (var i in keys_)
            {
                List<Tuple<string, string>> values = checkedFilesList[i];
                var uniqDirNames = values.Select(v => v.Item1).Distinct();
                List<Tuple<string, string>> valuesFinally = new List<Tuple<string, string>>();  
                foreach (var x_ in uniqDirNames)
                {
                   var test_ =  values.Where(v => v.Item1 == x_);
                   var len_ =  test_.Count();
                   valuesFinally.AddRange(test_);
                    /*****
                     * 
                     * Допилить отображение файлов.
                     * 
                     * 
                     * 
                     ****/
                    Console.WriteLine(test_);
                }
                TreeViewItem KeyOfDirectory = new TreeViewItem();
                KeyOfDirectory.Header = i.ToString();
                TreeViewItem oldfileName = null;
                foreach (var val in values)
                {
                    TreeViewItem dirName = new TreeViewItem();
                    dirName.Header = val.Item1;
                    TreeViewItem fileName = new TreeViewItem();
                    fileName.Header = val.Item2;
                    dirName.Items.Add(fileName);
                    KeyOfDirectory.Items.Add(dirName);
                }
                ParentItem.Items.Add(KeyOfDirectory);
            }
        }

        private void listView_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {

        }
    }
}

