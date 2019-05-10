using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OrangeCity_Console
{
    class Program
    {
        public static List<string> setup = new List<string>();
        public static List<string> TotDate = new List<string>();
        public static List<string> locs = new List<string>();
        public static string LocalDB, LocalNgpDB, line, path, typeprocess, Location, TotalFlowUnitMk, FlowUnitMk, RemoteDB, LogQuery, Key, SNO, logpath = "";
        public static string LogSerialNo, LogSIMNo;
        public static SqlConnection LocalDBconn = new SqlConnection();
        public static SqlConnection NGPDBconn = new SqlConnection();
        public static SqlConnection LocalNgpDBconn = new SqlConnection();
        //public static SqlCommand insertPressure;
        //public static SqlCommand insertFlow;
        public static List<LocationsID> results;
        //public static string[] allfiles;
        public static Int32 i, j = 1;
        public static decimal FlowRate, ActFlowRate, Consumption, Lat, Long, PressureValue, LastPeriod, CurrentPeriod = 0;
        public static double FlowUnit, TotalFlowUnit;
        //public static ConsoleKeyInfo cki;
        public static DateTime From_Date, To_Date;


        static void Main(string[] args)
        {
            path = "C:\\HWM\\SCADA\\NGP.txt";

            if (!File.Exists(path))
            {
                string[] lines = { "Title-", "Server-", "Database-", "UserName-", "Password-", "Local-", "Remote-", "Error-", "Mode (A/M)-", "Totalizer Units (m3/Lts)-", "Flow Rate (Lts.s/m3.h)-", "Totalizer Correction-", "Flow rate Correction-", "Interval (hrs)-", "Interval (Mins)-", "Channel No (Flow)-", "License No-", "License Status (Y/N/NA)-NA", "Transfer (FTP/Local)-", "FTP IP-", "FTP/WIndows Username-", "FTP/Windows Password-", "Delete (Y/N)-", "Local 2-" };

                System.IO.File.WriteAllLines(path, lines);
            }

            System.IO.StreamReader file =
              new StreamReader(path);
            while ((line = file.ReadLine()) != null)
            {
                int index = line.LastIndexOf("-");
                if (index > 0)
                    line = line.Substring(index + 1);
                if (!String.IsNullOrWhiteSpace(line))
                {
                    setup.Add(line);
                }
                else
                { j = 0; }

                //call your function here  
            }
            if (j != 0)
            {
                //string filename = String.Format("{0:yyyy-MM-dd}__{1}", DateTime.Now, "Log.txt");
                //string path = Path.Combine(setup[27], filename);
                ////logpath = setup[27] + "\\" + filename;
                //sw = new StreamWriter(path);

                //fs = new FileStream(path, System.IO.FileMode.Create);

                LocalDB = "Data Source=" + setup[1] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[2] + ";Persist Security Info=True;User ID=" + setup[3] + ";Password=" + setup[4] + "";
                Console.Title = setup[0];
                RemoteDB = "Data Source=" + setup[5] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[6] + ";Persist Security Info=True;User ID=" + setup[7] + ";Password=" + setup[8] + "";
                LocalNgpDB = "Data Source=" + setup[5] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[19] + ";Persist Security Info=True;User ID=" + setup[3] + ";Password=" + setup[4] + "";
                StartProcess();
            }
        }

        private static void StartProcess()
        {
            if (setup[9] == "A")
            {
                typeprocess = "A";
                int dt = Convert.ToInt32(setup[21]);
                dt = dt * -1;
                //From_Date = DateTime.Today.AddDays(-1);
                //To_Date = DateTime.Today;
                From_Date = DateTime.Today.AddDays(dt);
                To_Date = DateTime.Today.AddDays(1);
                Console.WriteLine("Trigger Auto from " + From_Date + " To " + To_Date);
                Console.WriteLine("--------------------------------------------------");
                //System.IO.StreamWriter sw = new System.IO.StreamWriter(fs);
                //System.Console.SetOut(sw);
                //sw.WriteLine("Trigger Auto from " + From_Date + " To " + To_Date);

            }
            else if (setup[9] == "M")
            {
                typeprocess = "M";
            }
            Object FromDate = From_Date;
            Object ToDate = To_Date;
            Object Type = typeprocess;

            starttoconvert(From_Date, To_Date, typeprocess);
        }

        private static void starttoconvert(DateTime from_Date, DateTime to_Date, string typeprocess)
        {
            Int32 Hours = Convert.ToInt32(setup[14]);
            Int32 Mins = Convert.ToInt32(setup[15]);
            Int32 FChannel = Convert.ToInt32(setup[16]);
            Int32 PChannel = Convert.ToInt32(setup[17]);
            Int32 InsertType = Convert.ToInt32(setup[20]);

            try
            {

                string query = "SELECT MIN(MeterRead) AS MINIMUM, MAX(MeterRead) AS MAXIMUM, SUM(Consumption) FROM dataExportEwp";
                string query_flow = "SELECT MAX (datapoints.value) FROM datapoints INNER JOIN sites ON datapoints.SiteID = sites.ID WHERE(datapoints.ChannelNumber = " + FChannel + ")";
                string disQuery = "SELECT sites.SiteID FROM sites INNER JOIN loggers ON sites.LoggerID = loggers.ID";
                string GetSNo = "SELECT SNo FROM Sites_SNo";
                string LoggerDetailsQuery = "SELECT loggers.LoggerSerialNumber, loggers.LoggerSMSNumber, sites.SiteID, sites.LatEast, sites.LongNorth FROM loggers INNER JOIN sites ON loggers.ID = sites.LoggerID";
                string pressurequery = "SELECT AVG (datapoints.value) FROM sites INNER JOIN loggers ON sites.LoggerID = loggers.ID INNER JOIN datapoints ON sites.LoggerID = datapoints.SiteID";

                LocalDBconn = new SqlConnection(LocalDB);
                //bwssbcon = new SqlConnection(connectionstring);

                NGPDBconn = new SqlConnection(RemoteDB);
                NGPDBconn.Open();

                LocalDBconn.Open();

                
                    SqlCommand GetSnoQ = new SqlCommand(GetSNo, NGPDBconn);
                    using (SqlDataReader oRedaer2 = GetSnoQ.ExecuteReader())
                    {
                        while (oRedaer2.Read())
                        {
                            string Sno = oRedaer2[0].ToString();
                            //Get Distinct Location Names
                            disQuery = disQuery + " where loggers.LoggerSerialNumber='" + Sno + "'";
                            SqlCommand QueryChk = new SqlCommand(disQuery, LocalDBconn);
                            results = new List<LocationsID>();
                            using (SqlDataReader oReader = QueryChk.ExecuteReader())
                            {
                                while (oReader.Read())
                                {
                                    LocationsID Loc = new LocationsID();
                                    Loc.LocID = oReader[0].ToString();
                                    results.Add(Loc);
                                }
                            }
                        }

                        //txt_locations.Text = results.Count.ToString();
                    }
                
               
                    //disQuery = "SELECT DISTINCT Mprn FROM dataExportEwp";
                    //SqlCommand QueryChk = new SqlCommand(disQuery, LocalDBconn);
                    //results = new List<LocationsID>();
                    //using (SqlDataReader oReader = QueryChk.ExecuteReader())
                    //{
                    //    while (oReader.Read())
                    //    {
                    //        LocationsID Loc = new LocationsID();
                    //        Loc.LocID = oReader[0].ToString();
                    //        results.Add(Loc);
                    //    }
                    //}
                
                //Get SNo from Remote Server
                

                LocalDBconn.Close();
                List<DateTime> dates = new List<DateTime>();

                for (var dt = from_Date; dt < to_Date; dt = dt.AddHours(Hours).AddMinutes(Mins))
                {
                    dates.Add(dt);
                }

                for (i = 0; i < results.Count; i++)
                {
                    for (int j = 0; j < dates.Count - 1; j++)
                    {
                        try
                        {

                            LocalDBconn.Open();
                            string from = dates[j].AddSeconds(1).ToString("M/d/yyyy HH:mm:ss");
                            string to = dates[j + 1].ToString("M/d/yyyy HH:mm:ss");
                            string prev = dates[j].AddHours(-Hours).AddMinutes(-Mins).AddSeconds(1).ToString("M/d/yyyy HH:mm:ss");

                            //if(setup[20] == "2") { Location = results[i].LocID.Substring(0, results[i].LocID.IndexOf('_')); }
                            //else if(setup[20] == "1")
                            Location = results[i].LocID;

                            //String query2_CurrentTotalizer = query + " Where Mprn = '" + results[i].LocID + "' AND datetime >='" + from + "' AND datetime<='" + to + "'";
                            String query2_CurrentTotalizer = query + " Where Mprn = 'khrone_2' AND datetime >='" + from + "' AND datetime<='" + to + "'";
                            String query2_CurrentFlow = query_flow + " AND (datapoints.DataTime >= '" + from + "') AND (datapoints.DataTime <= '" + to + "') AND (sites.SiteID = '" + Location + "')";
                            String query2_loggerDetails = LoggerDetailsQuery + " Where sites.SiteID = '" + Location + "' ";
                            string query2_Pressure = pressurequery + " where sites.SiteID = '" + Location + "' AND datapoints.ChannelNumber= " + PChannel + " AND datapoints.DataTime >='" + from + "' AND datapoints.DataTime<='" + to + "'";

                            SqlCommand CurrentTot = new SqlCommand(query2_CurrentTotalizer, LocalDBconn);
                            //Console.WriteLine(query2);
                            SqlCommand QueryFlow = new SqlCommand(query2_CurrentFlow, LocalDBconn);

                            SqlCommand LogDetails = new SqlCommand(query2_loggerDetails, LocalDBconn);

                            SqlCommand Pressure = new SqlCommand(query2_Pressure, LocalDBconn);
                            //Console.WriteLine(query2_flow);

                            using (SqlDataReader oReader2 = CurrentTot.ExecuteReader())
                            {
                                while (oReader2.Read())
                                {
                                    TotalFlowUnit = 1;
                                    TotalFlowUnitMk = "1";
                                    if (setup[10] == "m3") { TotalFlowUnit = 1; TotalFlowUnitMk = "1"; }
                                    else if (setup[10] == "Lts") { TotalFlowUnit = 1000; TotalFlowUnitMk = "0"; }

                                    CurrentPeriod = 0;
                                    if (String.IsNullOrEmpty(oReader2[1].ToString()))
                                    {
                                        CurrentPeriod = 0;
                                    }
                                    else
                                    {
                                        CurrentPeriod = Convert.ToDecimal(oReader2[1].ToString());
                                        if (setup[11] != "1")
                                        {
                                            CurrentPeriod = CurrentPeriod * Convert.ToDecimal(setup[12]) * Convert.ToDecimal(TotalFlowUnit);
                                        }
                                    }

                                    Consumption = 0;
                                    if (String.IsNullOrEmpty(oReader2[2].ToString()))
                                    {
                                        Consumption = 0;
                                    }
                                    else
                                    {
                                        Consumption = Convert.ToDecimal(oReader2[2].ToString());
                                    }
                                }
                            }

                            //Logger Details
                            using (SqlDataReader oReader4 = LogDetails.ExecuteReader())
                            {
                                while (oReader4.Read())
                                {
                                    if (String.IsNullOrEmpty(oReader4[1].ToString()))
                                    {
                                        LogSIMNo = "0";
                                    }
                                    else
                                    {
                                        LogSIMNo = oReader4[1].ToString();
                                    }

                                    if (String.IsNullOrEmpty(oReader4[0].ToString()))
                                    {
                                        LogSerialNo = "0";
                                    }
                                    else
                                    {
                                        LogSerialNo = oReader4[0].ToString();
                                    }

                                    if (String.IsNullOrEmpty(oReader4[3].ToString()))
                                    {
                                        Long = 0;
                                    }
                                    else
                                    {
                                        Long = Convert.ToDecimal(oReader4[3].ToString());
                                    }

                                    if (String.IsNullOrEmpty(oReader4[4].ToString()))
                                    {
                                        Lat = 0;
                                    }
                                    else
                                    {
                                        Lat = Convert.ToDecimal(oReader4[4].ToString());
                                    }
                                }

                            }

                            //Pressure
                            using (SqlDataReader oReader5 = Pressure.ExecuteReader())
                            {
                                while (oReader5.Read())
                                {

                                    if (String.IsNullOrEmpty(oReader5[0].ToString()))
                                    {
                                        PressureValue = 0;
                                    }
                                    else
                                    {
                                        PressureValue = Convert.ToDecimal(oReader5[0].ToString());
                                    }

                                }
                            }

                            //Flow Rate
                            using (SqlDataReader oReader3 = QueryFlow.ExecuteReader())
                            {
                                ActFlowRate = 0;
                                FlowRate = 0;
                                while (oReader3.Read())
                                {
                                    if (Consumption != 0)
                                    {
                                        decimal hrs = Hours * 60;
                                        hrs = hrs + Mins;
                                        if (setup[11] == "m3.h") { hrs = hrs / 60; }

                                        FlowRate = Consumption / hrs;
                                        FlowRate = FlowRate * Convert.ToDecimal(setup[13]);
                                    }
                                    ActFlowRate = Math.Round(FlowRate, 2);
                                }
                            }

                            if (LogSerialNo != "0")
                            {
                                if (InsertType == 2)
                                {
                                    NGPDBconn = new SqlConnection(RemoteDB);

                                    NGPDBconn.Open();

                                    string chkRows = "SELECT COUNT(*) FROM Site_Reading where seriel_number =" + LogSerialNo + " and DateTime ='" + to + "'";
                                    SqlCommand cmd = new SqlCommand(chkRows, NGPDBconn);
                                    int RecCount = (int)cmd.ExecuteScalar();
                                    if (RecCount > 0)
                                    {
                                        Double idflow = (Convert.ToDouble(LogSerialNo) * 10) + 1;
                                        Double idPress = (Convert.ToDouble(LogSerialNo) * 10) + 2;

                                        //string insertFlow = "INSERT INTO Site_Reading(seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',1,'D1a'," + idflow + ",'" + CurrentPeriod + "','" + to + "','" + ActFlowRate + "')";
                                        //string insertPressure = "INSERT INTO Site_Reading(seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',2,'A1'," + idPress + ",'" + PressureValue + "','" + to + "','')";
                                        string insertFlow = "UPDATE Site_Reading SET value ='" + CurrentPeriod + "' , FlowRate = '" + ActFlowRate + "' WHERE channel_index = 'D1a' AND seriel_number = " + LogSerialNo + " AND DateTime ='" + to + "'";
                                        string insertPressure = "UPDATE Site_Reading SET value ='" + PressureValue + "' , FlowRate = '' WHERE channel_index = 'A1' AND seriel_number = " + LogSerialNo + " AND DateTime ='" + to + "'";
                                        
                                        NGPDBconn = new SqlConnection(RemoteDB);
                                        NGPDBconn.Open();

                                        SqlCommand InFlow = new SqlCommand(insertFlow, NGPDBconn);
                                        InFlow.ExecuteNonQuery();
                                        SqlCommand InPress = new SqlCommand(insertPressure, NGPDBconn);
                                        InPress.ExecuteNonQuery();

                                        NGPDBconn.Close();
                                        Console.WriteLine("Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} -> UPDATE", Location, to, PressureValue, ActFlowRate, CurrentPeriod);

                                    }
                                    else
                                    {
                                        Double idflow = (Convert.ToDouble(LogSerialNo) * 10) + 1;
                                        Double idPress = (Convert.ToDouble(LogSerialNo) * 10) + 2;

                                        string insertFlow = "INSERT INTO Site_Reading(seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',1,'D1a'," + idflow + ",'" + CurrentPeriod + "','" + to + "','" + ActFlowRate + "')";
                                        string insertPressure = "INSERT INTO Site_Reading(seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',2,'A1'," + idPress + ",'" + PressureValue + "','" + to + "','')";

                                        NGPDBconn = new SqlConnection(RemoteDB);
                                        NGPDBconn.Open();

                                        SqlCommand InFlow = new SqlCommand(insertFlow, NGPDBconn);
                                        InFlow.ExecuteNonQuery();
                                        SqlCommand InPress = new SqlCommand(insertPressure, NGPDBconn);
                                        InPress.ExecuteNonQuery();

                                        NGPDBconn.Close();
                                        Console.WriteLine("Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} -> UPDATE", Location, to, PressureValue, ActFlowRate, CurrentPeriod);
                                    }
                                    

                                }
                                else if (InsertType == 1)
                                {
                                    NGPDBconn = new SqlConnection(RemoteDB);

                                    NGPDBconn.Open();

                                    string chkRows = "SELECT * FROM Site_Reading where seriel_number =" + LogSerialNo + ""; 
                                    SqlCommand cmd = new SqlCommand(chkRows, NGPDBconn);
                                    int RecCount = (int)cmd.ExecuteScalar();
                                    if (RecCount > 0)
                                    {
                                        string updateFlow = "UPDATE Site_Reading SET value ='" + CurrentPeriod + "' ,DateTime ='" + to + "', FlowRate = '" + ActFlowRate + "' WHERE channel_index = 'D1a' AND seriel_number = " + LogSerialNo + "";
                                        string updatePress = "UPDATE Site_Reading SET value ='" + PressureValue + "' ,DateTime ='" + to + "', FlowRate = '' WHERE channel_index = 'A1' AND seriel_number = " + LogSerialNo + "";
                                        //NGPDBconn.Open();

                                        SqlCommand InFlow = new SqlCommand(updateFlow, NGPDBconn);
                                        InFlow.ExecuteNonQuery();
                                        SqlCommand InPress = new SqlCommand(updatePress, NGPDBconn);
                                        InPress.ExecuteNonQuery();

                                        //NGPDBconn.Close();
                                    }
                                    else if(RecCount == 0)
                                    {
                                        
                                        Double idflow = (Convert.ToDouble(LogSerialNo) * 10) + 1;
                                        Double idPress = (Convert.ToDouble(LogSerialNo) * 10) + 2;

                                        string insertFlow = "INSERT INTO Site_Reading(seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',1,'D1a'," + idflow + ",'" + CurrentPeriod + "','" + to + "','" + ActFlowRate + "')";
                                        string insertPressure = "INSERT INTO Site_Reading(seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',2,'A1'," + idPress + ",'" + PressureValue + "','" + to + "','')";

                                        //NGPDBconn = new SqlConnection(RemoteDB);
                                        //NGPDBconn.Open();

                                        SqlCommand InFlow = new SqlCommand(insertFlow, NGPDBconn);
                                        InFlow.ExecuteNonQuery();
                                        SqlCommand InPress = new SqlCommand(insertPressure, NGPDBconn);
                                        InPress.ExecuteNonQuery();

                                        
                                        Console.WriteLine("Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} -> INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod);
                                    }
                                    NGPDBconn.Close();
                                }
                            }

                            Console.WriteLine("--------------------------------------------------");

                            LocalDBconn.Close();
                        }
                        catch (Exception Ex)
                        {
                            Console.WriteLine("Exception Occurred :{0},{1}", Ex.Message, Ex.StackTrace.ToString());
                            // Console.ReadLine();
                            LocalDBconn.Close();
                        }

                    }

                    LocalDBconn.Close();
                }
            }
            catch (Exception Ex)
            {
                Console.WriteLine("Exception Occurred :{0},{1}", Ex.Message, Ex.StackTrace.ToString());
                // Console.ReadLine();
                LocalDBconn.Close();
            }
        }
    }
}



