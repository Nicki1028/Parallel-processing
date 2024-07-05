
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;
using CSV1;


namespace 平行處理
{
    internal class Program
    {

        static async Task Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            // 原始尚未分割 讀取  讀+寫
            // 一萬筆資料 26ms    1902ms
            // 十萬筆資料 313ms   16487ms
            // 百萬筆資料 2801ms  178555ms
            // 千萬筆資料 39202ms 3056861ms

            // 一萬筆分割 讀取  讀+寫
            // 十萬筆資料 119ms 15938ms
            // 百萬筆資料 1279ms 168971ms
            // 千萬筆資料 39903ms 1707156ms

            // 一萬筆parelle 讀取  讀+寫
            // 十萬筆資料 119ms 17088ms
            // 百萬筆資料 1279ms 3ms         3987ms
            // 千萬筆資料 39903ms 1707156ms  32375ms
            int lineNum = 10001100;
            int batchSize = 100000;
            string file = @"C:\Users\USER\Downloads\MOCK_DATA-千萬.csv";
            //SerializedBatch(lineNum, batchSize);


            await ParellizedBatch(lineNum, batchSize, file);

            stopwatch.Stop();
            Console.WriteLine(stopwatch.ElapsedMilliseconds);

            Console.ReadKey();
        }
        //static void SerializedBatch(int lineNum, int batchSize)
        //{
        //    CsvHelper csvHelper = new CsvHelper();
        //    int batchNum = lineNum / batchSize;

        //    for (int i = 0; i < batchNum; i++)
        //    {
        //        List<MemberModel> data = csvHelper.Read<MemberModel>(@"C:\Users\USER\Downloads\MOCK_DATA-千萬.csv", false, i * batchSize + 1, batchSize);
        //        //csvHelper.Writelist<MemberModel>(data, @"C:\Users\USER\Downloads\MOCK_DATA-千萬_test1.csv");
        //        GC.Collect();
        //    }
        //}

        static async Task ParellizedBatch(int lineNum, int batchSize, string filename)
        {
            CsvHelper csvHelper = new CsvHelper();
            List<Task> task = new List<Task>();
            int batchNum = lineNum / batchSize;

            for (int i = 0; i < batchNum; i++)
            {
                int index = i;
                task.Add(Task.Run(async () =>
                {
                    List<MemberModel> data = csvHelper.ReadCSV<MemberModel>(filename, index * batchSize + 1, batchSize);
                    await csvHelper.Writelist(data, $@"C:\Users\USER\Downloads\MOCK_DATA-千萬_parelltest{index}.csv");
                    GC.Collect();
                }));
            }
            await Task.WhenAll(task);
        }

        //static async Task ParallelizedBatch(int lineNum, int batchSize, string fileName)
        //{
        //    List<Task> tasks = new List<Task>();

        //    int batchNum = lineNum / batchSize;

        //    // var let
        //    for (int i = 0; i < batchNum; i++)
        //    {
        //        int fileIndex = i;
        //        tasks.Add(Task.Run(() =>
        //        {
        //            var data = CsvHelper.ReadCSV<MemberModel>(fileName, fileIndex * batchSize + 1, batchSize);
        //            CsvHelper.WriteCSV($@"C:\Users\USER\Downloads\MOCK_DATA-千萬_parelltest{fileIndex}.csv", data, append: false, createDir: true);
        //            GC.Collect();
        //        }));
        //        //Thread.Sleep(1000);
        //    }

        //    await Task.WhenAll(tasks);

    }
}

