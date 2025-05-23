﻿
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CSV_Library;
using CSV1;


namespace 平行處理
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8; 
            int task1 = 0;
            int task2 = 0;  

            for(int i=0;i<20;i++)
            {
                if(i%2==0)
                {
                    task1+=10;
                }
                else
                {
                    task2+=10;  
                }

                Console.Clear();    
                for(int j=0;j<task1;j++)
                {
                    Console.Write("▮");  
                }
                Console.WriteLine();
                for (int j = 0; j < task2; j++)
                {
                    Console.Write("▮");
                }
                Console.WriteLine();

               // Thread.Sleep(5);
            }



            // 原始尚未分割   讀取    讀+寫
            // 一萬筆資料     26ms    1902ms
            // 十萬筆資料     313ms   16487ms
            // 百萬筆資料     2801ms  178555ms
            // 千萬筆資料     39202ms 3056861ms

            // 一萬筆分割    讀取     讀+寫
            // 十萬筆資料    359ms    873ms
            // 百萬筆資料    3071ms   7584ms
            // 千萬筆資料    32725ms  77044ms

            // 一萬筆平行    讀取     讀+寫
            // 十萬筆資料    362ms    899ms
            // 百萬筆資料    1491ms   3257ms
            // 千萬筆資料    21302ms  36571ms

            //int lineNum = 70_000_000;
            //int batchSize = 2_500_000;
            //string file = @"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Input\MOCK_DATA-七千萬.csv";

            //if (Directory.Exists(@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output"))
            //{
            //    Directory.Delete(@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output", true);
            //    Directory.CreateDirectory(@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output");
            //}
            ////SerializedBatch(lineNum, batchSize, file);
            //await ParellizedBatch(lineNum, batchSize, file);
            //AddData addData = new AddData();
            //addData.Add();

            Console.ReadKey();
        }
        static void SerializedBatch(int lineNum, int batchSize, string filename)
        {
            CsvHelper csvHelper = new CsvHelper();
            int batchNum = lineNum / batchSize;

            for (int i = 0; i < batchNum; i++)
            {
                List<MemberModel> data = csvHelper.Read<MemberModel>(filename, i * batchSize + 1, batchSize);
                csvHelper.Writelist<MemberModel>(data, @"C:\Users\USER\Downloads\MOCK_DATA-千萬_test1.csv");
                //GC.Collect();
            }
        }

        static async Task ParellizedBatch(int lineNum, int batchSize, string filename)
        {
            Stopwatch stopwatchs = new Stopwatch();
            stopwatchs.Start();

            List<double> read = new List<double>();
            List<double> write = new List<double>();
            CsvHelper csvHelper = new CsvHelper();
            List<Task> task = new List<Task>();
            int batchNum = lineNum / batchSize;
          
            for (int i = 0; i < batchNum; i++)
            {
                int index = i;
                task.Add(Task.Run(() =>
                {
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();

                    List<MemberModel> data = csvHelper.Read<MemberModel>(filename, index * batchSize + 1, batchSize);
                    //List<MemberModel> data = CSV.Read<MemberModel>(filename, index * batchSize + 1, batchSize);
                    stopwatch.Stop();
                    double readTime = Math.Round((stopwatch.ElapsedMilliseconds / 1000f), 2);
                    //Console.WriteLine($"第{index + 1}任務完成! 讀取時間:{readTime}");

                    read.Add(readTime);

                    stopwatch.Restart();
                    
                    csvHelper.Writelist(data, $@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output\MOCK_DATA-兩千萬_parelltest優化{index}.csv");
                    //CSV.Write<MemberModel>($@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output\MOCK_DATA-兩千萬_parelltest優化{index}.csv",data);
                    stopwatch.Stop();
                    double writeTime = Math.Round((stopwatch.ElapsedMilliseconds / 1000f), 2);
                    //Console.WriteLine($"第{index + 1}任務完成! 寫入時間:{writeTime} | 任務完成時間:{Math.Round(readTime + writeTime, 2)} s");

                    write.Add(writeTime);
                    Console.WriteLine($"第{index + 1}任務完成! 讀取時間:{readTime} | 寫入時間:{writeTime} | 任務完成時間:{Math.Round(readTime + writeTime, 2)} s");

                    data.Clear();
                    data = null;
                    GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
                    GC.WaitForPendingFinalizers();
                    GC.Collect(); // 第二次確保清空

                }));
            }
            await Task.WhenAll(task);
            stopwatchs.Stop();
            Console.WriteLine($"平均讀取時間:{read.Median()} 平均寫入時間:{write.Median()} 總完成時間:{Math.Round((stopwatchs.ElapsedMilliseconds / 1000f), 2)} s");
        }
        
    }
}

