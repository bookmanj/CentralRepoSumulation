using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace CentralRepoSumulation
{
    class CommandLineOptions
    {
        [Option('l', "driveLetter", Required = false, HelpText = "Drive letter to be use when creating the local cache  (Default = C)", Default = "C")]
        public string DriveLetter { get; set; }
        [Option('b', "bucketName", Required = false, HelpText = "S3 bucket name to use for download and upload operations (Default = customer-dev-env", Default = "customer-dev-env")]
        public string BucketName { get; set; }
        [Option('a', "appName", Required = false, HelpText = "Application folder name (Default = app", Default = "app")]
        public string AppName { get; set; }
        [Option('c', "concurrent", Required = false, HelpText = "Number of concurrent applications (Default = 1)", Default = 1)]
        public int ConcurrentCount { get; set; }
        [Option('d', "downloadOff", Required = false, HelpText = "Disable S3 downloading (Default = false)", Default = false)]
        public bool DownloadOff { get; set; }
        [Option('u', "uploadOff", Required = false, HelpText = "Disable S3 uploading (Default = false)", Default = false)]
        public bool UploadOff { get; set; }
        [Option('o', "outputPath", Required = false, HelpText = "Output path to place the csv file  (Default = C:\\s3-temp\\pocResult.csv)", Default = "C:\\s3-temp\\pocResult.csv")]
        public string OutputPath { get; set; }
    }
}
