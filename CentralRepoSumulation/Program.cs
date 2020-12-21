using Amazon.S3;
using Amazon.S3.Transfer;
using System;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace CentralRepoSumulation
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //Console.WriteLine($".Net Core 3.1 = {System.Net.ServicePointManager.DefaultConnectionLimit}");
            System.Net.ServicePointManager.DefaultConnectionLimit = Int32.MaxValue;
            //System.Net.ServicePointManager.DefaultConnectionLimit = 100;
            Console.WriteLine($".Net Core 3.1 = {System.Net.ServicePointManager.DefaultConnectionLimit}");
            // initialize timer
            var timer = new Stopwatch();
            timer.Start();

            // Create an S3 client object.
            var s3Client = new AmazonS3Client();

            // get source
            if (GetSourceAndDest(args, out String source, out String dest))
            {
                Console.WriteLine($"copying source: {source} to dest: {dest}");

                if (GetBucketNameAndS3Folder(dest, out String bucketName, out String s3Path))
                {
                    //Console.WriteLine($"bucketName: {bucketName}, s3Path: {s3Path}");

                    // create of list of files to upload to S3
                    string[] files = Directory.GetFiles(source, "*.*", SearchOption.AllDirectories);


                    //foreach (string currentFile in files)
                    Parallel.ForEach(files, (currentFile) =>
                    {

                        FileInfo info = new FileInfo(currentFile);
                        string fileToBackup = info.FullName; // file to upload
                        string s3FileName = info.Name;  // fileName - need to add path with file name                   

                        string tempPath = info.DirectoryName.Remove(0, source.Length + 1);
                        string s3Folder = s3Path + "/" + tempPath;

                        //Console.WriteLine($"fileToBackup: {fileToBackup}, s3FileName: {s3FileName}, bucketName: {bucketName + @"/" + s3Folder}");

                        var uploadRequest = new TransferUtilityUploadRequest
                        {
                            FilePath = fileToBackup,
                            Key = s3FileName,
                            BucketName = bucketName + @"/" + s3Folder,
                            CannedACL = S3CannedACL.NoACL // gives the ownler full controll and no-one else
                        };

                        UploadFiles(s3Client, uploadRequest);

                        //Console.WriteLine($"Processing {info.FullName} on thread {Thread.CurrentThread.ManagedThreadId}");
                        //}
                    });

                    // Get the elapsed time as a TimeSpan value.
                    TimeSpan ts = timer.Elapsed;

                    // Format and display the TimeSpan value.
                    string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                        ts.Hours, ts.Minutes, ts.Seconds,
                        ts.Milliseconds / 10);
                    Console.WriteLine("RunTime " + elapsedTime);
                }
            }
        }

        //  Method to parse the command line
        private static Boolean GetSourceAndDest(string[] args, out String source, out String dest)
        {
            Boolean retval = false;
            source = String.Empty;
            dest = String.Empty;
            if (args.Length <= 1)
            {
                Console.WriteLine("\nNo arguments specified. Please provide the local source directory and S3 destination." +
                  "\n e.g. <exe> <drive>:\\<folder path> s3://<bucket>/<folder path>" +
                  "\n S3CreateAndList.exe c:\\s3-temp\app1 s3://crt01/app1");
                source = String.Empty;
                dest = String.Empty;
                retval = false;
            }
            else if (args.Length == 2)
            {
                source = args[0];
                dest = args[1];
                retval = true;
            }
            else
            {
                Console.WriteLine("\nToo many arguments specified.");
                Environment.Exit(1);
            }
            return retval;
        }

        // get the bucketName and S3Path
        public static Boolean GetBucketNameAndS3Folder(string url, out String bucketName, out String s3Path)
        {
            Boolean retval = false;
            bucketName = String.Empty;
            s3Path = String.Empty;
            // regex to get bucketname and s3Path
            Regex r = new Regex(@"^(?<proto>\w+)://(?<host>.+)/(?<path>\w+)",
                          RegexOptions.None, TimeSpan.FromMilliseconds(150));

            // use regex on string
            Match m = r.Match(url);

            // if successfull set global vars
            if (m.Success)
            {                
                bucketName = m.Result("${host}");
                s3Path = m.Result("${path}");
                retval = true;
            }
            return retval;
        }

        // file upload
        private static async Task UploadFiles(IAmazonS3 s3Client, TransferUtilityUploadRequest uploadRequest)
        {
            TransferUtility utility = new TransferUtility(s3Client);
            utility.Upload(uploadRequest);
        }
    }
}
