using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using CommandLine;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using System.Text;

namespace CentralRepoSumulation
{
    internal class PocData
    {
        public string AppName { get; set; }
        public string AppFiles { get; set; }
        public string AppFilesSize { get; set; }
        public string AppFilesDownloadTime { get; set; }
        public string AppFilesDownloadSpeed { get; set; }
        public string AppFilesUploadTime { get; set; }
        public string AppFilesUploadSpeed { get; set; }
        public string ConcurrentApps { get; set; }
    }

    class Program
    {

        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USEast1;
        private const string wildCard = "*.*";

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<CommandLineOptions>(args)
                .WithParsed(opts => DoSomeWork(opts))
                .WithNotParsed((errs) => HandleParseError(errs));
        }


        private static void DoSomeWork(CommandLineOptions opts)
        {            
            // init and start the total stopwatch timer
            Stopwatch total = new Stopwatch();
            total.Start();

            // write out all command vars that we will be using
            Console.WriteLine($"Drive letter: {opts.DriveLetter}, BucketName: {opts.BucketName}," +
                $" AppName: {opts.AppName}, ConcurrentAppCount: {opts.ConcurrentCount}," +
                $" DownloadOff: {opts.DownloadOff}, UploadOff: {opts.UploadOff}\n");

            // create poc data list and populate app name
            List<PocData> pocData = new List<PocData>();
            foreach (int index in Enumerable.Range(1, opts.ConcurrentCount))
            {
                UpdateCsvData(pocData, $"{opts.AppName}-{index}", "", "", "", "", opts.ConcurrentCount.ToString());
            }

            // Create an S3 client object.
            var s3Client = new AmazonS3Client(bucketRegion);

            if (!opts.DownloadOff)
            {
                // init stopwatch timer and create List<Action>
                Stopwatch dl = new Stopwatch();
                List<Action> actions = new List<Action>();

                // create local cache directory and populate the List<Action> with the DownloadDirectoryAsync method
                foreach (int index in Enumerable.Range(1, opts.ConcurrentCount))
                {
                    // local cache directory
                    string localCache = $"{opts.DriveLetter}:\\local-cache\\{opts.BucketName}\\{opts.AppName}-{index}";
                    ManageFolder(localCache);

                    // populate the List<Action> with the DownloadDirectoryAsync method and output stopwatch elapsed time
                    actions.Add(() => {
                        DownloadDirAsync(s3Client, opts.BucketName, "app-init", localCache).Wait();
                        Console.WriteLine($"Local cache {localCache} - download time: {dl.Elapsed.TotalSeconds}" +
                            $"\n Transfer speed: {TransferSpeed(pocData, localCache, dl.Elapsed.TotalSeconds, $"{opts.AppName}-{index}", "down")}");
                    });
                }

                // start the dowload timer and kick off the DownloadDirectoryAsync method in parallel
                dl.Start();
                Parallel.ForEach(actions, new ParallelOptions
                {
                    MaxDegreeOfParallelism = opts.ConcurrentCount
                }, action => action());
                
                // addd a line between up and download runs
                Console.WriteLine("");

                // wait 5 secs before upload
                System.Threading.Thread.Sleep(5000);
            }

            if (!opts.UploadOff)
            {
                // init stopwatch timer and List<Action> for upload prep
                Stopwatch ul = new Stopwatch();
                List<Action> uploadPrepActions = new List<Action>();

                // create local cache directory and populate the List<Action> with the DownloadDirectoryAsync method
                foreach (int index in Enumerable.Range(1, opts.ConcurrentCount))
                {
                    // local cache directory                
                    string localCache = $"{opts.DriveLetter}:\\local-cache\\{opts.BucketName}\\{opts.AppName}-{index}";

                    // populate the List<Action> with the DownloadDirectoryAsync method and output stopwatch elapsed time
                    uploadPrepActions.Add(() => {
                        UploadDirAsync(s3Client, localCache, $"{ opts.BucketName}/{opts.AppName}-{index}").Wait();
                        Console.WriteLine($"S3 AppDir: {opts.BucketName}/{opts.AppName}-{index} - upload time: {ul.Elapsed.TotalSeconds}" +
                            $"\n Transfer speed: {TransferSpeed(pocData, localCache, ul.Elapsed.TotalSeconds, $"{opts.AppName}-{index}", "up")}");
                    });
                }

                // start the dowload timer and kick off the DownloadDirectoryAsync method in parallel
                ul.Start();
                Parallel.ForEach(uploadPrepActions, new ParallelOptions
                {
                    MaxDegreeOfParallelism = opts.ConcurrentCount
                }, action => action());               
            }

            // stop timer and output results
            total.Stop();
            Console.WriteLine("\nTotal RunTime " + total.Elapsed.TotalSeconds);

            // write csv
            WriteCsv(pocData, opts.OutputPath);
        }

        // s3 upload directory
        private static async Task UploadDirAsync(IAmazonS3 s3Client, string localDirectory, string s3Path)
        {
            try
            {
                // create the directoryTransferUtility object
                var directoryTransferUtility = new TransferUtility(s3Client);                
                
                // Upload a directory
                //await directoryTransferUtility.UploadDirectoryAsync(localDirectory, s3Path, wildCard, SearchOption.AllDirectories);
                await directoryTransferUtility.UploadDirectoryAsync(localDirectory, s3Path);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered ***. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
        }

        
        // S3 download
        private static async Task DownloadDirAsync(IAmazonS3 s3Client, string bucketName, string s3Directory, string localDirectory)
        {
            try
            {
                // create the directoryTransferUtility object
                var directoryTransferUtility = new TransferUtility(s3Client);

                // download a s3 directory
                await directoryTransferUtility.DownloadDirectoryAsync(bucketName, s3Directory, localDirectory);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered ***. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }            
        }


        // manage local cache folder
        private static void ManageFolder(string path)
        {
            try
            {
                // Determine whether the directory exists.
                if (Directory.Exists(path))
                {
                    //Console.WriteLine($"The path {path} exists already.");
                    return;
                }

                // Try to create the directory.
                DirectoryInfo di = Directory.CreateDirectory(path);
                Console.WriteLine($"The directory {path} was created successfully at {Directory.GetCreationTime(path)}.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"The process failed: {e.ToString()}");
            }
            finally { }
        }


        // calulate transfer speed
        private static float TransferSpeed(List<PocData> pocData, string dirPath, double seconds, string appName, string type)
        {
            DirectoryInfo info = new DirectoryInfo(dirPath);
            long totalSize = info.EnumerateFiles().Sum(file => file.Length);

            float result = (float)(ConvertBytesToMegabytes(totalSize) / Convert.ToDouble(seconds));

            if (type == "up") { UpdateCsvData(pocData, appName, "", "", seconds.ToString(), result.ToString()); }

            if (type == "down") { UpdateCsvData(pocData, appName, seconds.ToString(), result.ToString(), "", ""); }

            return result;
        }


        private static double ConvertBytesToMegabytes(long bytes)
        {
            return (bytes / 1024f) / 1024f;
        }


        // populate csv data
        private static void UpdateCsvData(List<PocData> pocData, string appName, string dlTime = "", string dlSpeed = "", string ulTime = "", string ulSpeed = "", string concurrentApps = "")
        {
            if (!String.IsNullOrEmpty(appName))
            {
                if (String.IsNullOrEmpty(dlTime) && String.IsNullOrEmpty(ulTime))
                {
                    pocData.Add(new PocData()
                    {
                        AppName = appName,
                        AppFiles = "50 1MB + 50 10MB files",
                        AppFilesSize = "550MB",
                        AppFilesDownloadTime = "",
                        AppFilesDownloadSpeed = "",
                        AppFilesUploadTime = "",
                        AppFilesUploadSpeed = "",
                        ConcurrentApps = concurrentApps
                    });
                }
                else if (!String.IsNullOrEmpty(dlTime) && String.IsNullOrEmpty(ulTime))
                {
                    foreach (var data in pocData.Where(w => w.AppName == appName))
                    {
                        data.AppFilesDownloadTime = dlTime;
                        data.AppFilesDownloadSpeed = dlSpeed;
                    }
                }
                else if (String.IsNullOrEmpty(dlTime) && !String.IsNullOrEmpty(ulTime))
                {
                    foreach (var data in pocData.Where(w => w.AppName == appName))
                    {
                        data.AppFilesUploadTime = ulTime;
                        data.AppFilesUploadSpeed = ulSpeed;
                    }
                }
            }           
        }


        // write csv
        public static void WriteCsv(List<PocData> pocData, string outputPath)
        {
            using (var mem = new MemoryStream())
            using (var writer = new StreamWriter(mem))
            //using (var csvWriter = new CsvWriter(writer, System.Globalization.CultureInfo.CurrentCulture))
            using (var csvWriter = new CsvWriter(writer, System.Globalization.CultureInfo.CurrentCulture))
            {
                csvWriter.Configuration.Delimiter = ",";
                csvWriter.Configuration.HasHeaderRecord = true;
                csvWriter.Configuration.AutoMap<PocData>();

                csvWriter.WriteHeader<PocData>();
                csvWriter.NextRecord();
                csvWriter.WriteRecords(pocData);

                writer.Flush();
                var result = Encoding.UTF8.GetString(mem.ToArray());
                //Console.WriteLine(result);

                if (File.Exists(outputPath))
                {
                    using (System.IO.StreamWriter file =
                        new System.IO.StreamWriter(outputPath, true))
                    {
                        file.Write(result);
                    }
                }
                else { System.IO.File.WriteAllText(outputPath, result); }
                
            }
        }


        // handle commandline exception
        private static void HandleParseError(IEnumerable errs)
        {
            Console.WriteLine("Command Line parameters provided were not valid!");
        }
    }
}
