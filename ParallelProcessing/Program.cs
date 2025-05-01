
using CSV1;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace 平行處理
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            int lineNum = 70_000_000;
            int batchSize = 2_000_000;
            CsvHelper csvHelper = new CsvHelper();
            int batchNum = lineNum / batchSize;
            string file = @"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Input\MOCK_DATA-七千萬.csv";
            List<double> read = new List<double>();
            List<double> write = new List<double>();

            if (Directory.Exists(@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output"))
            {
                Directory.Delete(@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output", true);
                Directory.CreateDirectory(@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output");
            }
            Stopwatch stopwatchs = new Stopwatch();
            stopwatchs.Start();

            await Parallel.ForAsync(0, batchNum, new ParallelOptions() {MaxDegreeOfParallelism = 5 }, (index, token) =>
            {
                          
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                List<MemberModel> data = csvHelper.Read<MemberModel>(file, index * batchSize + 1, batchSize);              
                stopwatch.Stop();
 
                double readTime = Math.Round((stopwatch.ElapsedMilliseconds / 1000f), 2);
                read.Add(readTime);

                stopwatch.Restart();
                csvHelper.Writelist(data, $@"C:\Users\USER\Desktop\CSharp\平行處理\平行處理\Output\MOCK_DATA-兩千萬_parelltest優化{index}.csv");               
                stopwatch.Stop();
                double writeTime = Math.Round((stopwatch.ElapsedMilliseconds / 1000f), 2);
                write.Add(writeTime);
                Console.WriteLine($"第{index + 1}任務完成! 讀取時間:{readTime} | 寫入時間:{writeTime} | 任務完成時間:{Math.Round(readTime + writeTime, 2)} s");

                data.Clear();
                data = null;
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
                GC.WaitForPendingFinalizers();
                GC.Collect(); // 第二次確保清空

                return ValueTask.CompletedTask;

            });

            Console.WriteLine("所有任務完成");
            stopwatchs.Stop();
            Console.WriteLine($"平均讀取時間:{read.Median()} 平均寫入時間:{write.Median()} 總完成時間:{Math.Round((stopwatchs.ElapsedMilliseconds / 1000f), 2)} s");
            Console.ReadKey();
        }
        static async Task ParellizedBatch(int lineNum, int batchSize, string filename)
        {
            Stopwatch stopwatchs = new Stopwatch();
            stopwatchs.Start();

            List<double> read = new List<double>();
            List<double> write = new List<double>();
            CsvHelper csvHelper = new CsvHelper();
            
            int batchNum = lineNum / batchSize;
            List<Task> task = new List<Task>();
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

