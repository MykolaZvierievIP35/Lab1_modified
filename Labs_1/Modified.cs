using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Labs_1
{
    internal class Modified
    {
        private static ILogger<Modified>? _logger;
        private static readonly string BasePath = @"..\..\..\";
        private static readonly string InputFileName = Path.Combine(BasePath, "file_A.txt");
        private static readonly string OutputFileName = Path.Combine(BasePath, "file_A_sorted.txt");
        private static readonly string FileB = Path.Combine(BasePath, "file_B.tmp");
        private static readonly string FileC = Path.Combine(BasePath, "file_C.tmp");
        private static readonly string PresortedSeriesFileName = Path.Combine(BasePath, "presorted_series.tmp");
        private static readonly string MergedSeriesFileName = Path.Combine(BasePath, "merged_series.tmp");

        private static void Main()
        {
            using var loggerFactory = CreateLoggerFactory();
            _logger = loggerFactory.CreateLogger<Modified>();

            try
            {
                const int chunkSize = 100 * 1024 * 1024;
                const int fileSize = 1000 * 1024 * 1024;
                const int averageElementSize = 10;
                const int numberOfElements = fileSize / averageElementSize;
                const string separator = "###";

                GenerateData(InputFileName, numberOfElements);

                var sorter = new ExternalMergeSorter(
                    loggerFactory.CreateLogger<ExternalMergeSorter>(),
                    FileB,
                    FileC,
                    separator
                );

                PerformSorting(sorter, chunkSize);

                _logger.LogInformation("Сортування завершено успішно.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Сталася помилка під час сортування.");
            }
        }

        private static ILoggerFactory CreateLoggerFactory()
        {
            return LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });
        }

        private static void GenerateData(string filePath, int numberOfElements)
        {
            var dataGenerator = new DataGenerator();
            dataGenerator.GenerateRandomData(filePath, numberOfElements);
        }

        private static void PerformSorting(ExternalMergeSorter sorter, int chunkSize)
        {
            var stopwatch = Stopwatch.StartNew();

            sorter.CreatePresortedSeries(InputFileName, PresortedSeriesFileName, chunkSize);
            sorter.Sort(PresortedSeriesFileName, OutputFileName, MergedSeriesFileName);

            File.Delete(PresortedSeriesFileName);
            stopwatch.Stop();

            _logger.LogInformation(
                "Сортування завершено за {0} хвилин, {1} секунд, {2} мілісекунд.",
                stopwatch.Elapsed.Minutes,
                stopwatch.Elapsed.Seconds,
                stopwatch.Elapsed.Milliseconds
            );
        }
    }

    public class DataGenerator
    {
        private readonly ILogger<DataGenerator> _logger;

        public DataGenerator()
        {
            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });
            _logger = loggerFactory.CreateLogger<DataGenerator>();
        }

        public void GenerateRandomData(string filePath, int numberOfElements)
        {
            _logger.LogInformation("Генерація файлу з випадковими даними: {FilePath}", filePath);

            var random = new Random();
            char[] buffer = new char[10];

            using var writer = new StreamWriter(filePath, false, Encoding.UTF8, 65536);

            for (int i = 0; i < numberOfElements; i++)
            {
                var number = random.Next(100_000_000);
                WriteNumber(writer, buffer, number);
            }

            _logger.LogInformation("Генерація даних завершена.");
        }

        private void WriteNumber(StreamWriter writer, char[] buffer, int number)
        {
            var span = buffer.AsSpan();
            number.TryFormat(span, out var charsWritten);
            writer.WriteLine(span.Slice(0, charsWritten));
        }
    }

    public class ExternalMergeSorter(
        ILogger<ExternalMergeSorter> logger,
        string fileB,
        string fileC,
        string separator
    )
    {

        public string CreatePresortedSeries(string inputFilePath, string presortedSeriesFileName, int chunkSize)
        {
            logger.LogInformation("Створення попередньо відсортованих серій з файлу: {FilePath}", inputFilePath);

            int[] buffer = new int[chunkSize / sizeof(int)];
            long currentBufferSize = 0;
            int bufferIndex = 0;

            using var reader = new StreamReader(inputFilePath);
            using var writer = new StreamWriter(presortedSeriesFileName, false, Encoding.UTF8, 65536);

            string? line;

            while ((line = reader.ReadLine()) != null)
            {
                ProcessLine(line, buffer, ref bufferIndex, ref currentBufferSize, chunkSize, writer);
            }

            if (bufferIndex > 0)
            {
                SortAndWriteBuffer(buffer, writer, bufferIndex);
            }

            logger.LogInformation("Початкові серії створено: {FilePath}", presortedSeriesFileName);
            return presortedSeriesFileName;
        }

        private void ProcessLine(string line, int[] buffer, ref int bufferIndex, ref long currentBufferSize, int chunkSize, StreamWriter writer)
        {
            if (int.TryParse(line.Trim(), out int value))
            {
                buffer[bufferIndex++] = value;
                currentBufferSize += sizeof(int);

                if (currentBufferSize >= chunkSize)
                {
                    SortAndWriteBuffer(buffer, writer, bufferIndex);
                    bufferIndex = 0;
                    currentBufferSize = 0;
                }
            }
        }

        private void SortAndWriteBuffer(int[] buffer, StreamWriter writer, int count)
        {
            Array.Sort(buffer, 0, count);
            WriteBufferToFile(buffer, writer, count);
        }

        private void WriteBufferToFile(int[] buffer, StreamWriter writer, int count)
        {
            char[] numberBuffer = new char[11];
            var span = numberBuffer.AsSpan();

            for (int i = 0; i < count; i++)
            {
                if (buffer[i].TryFormat(span, out var charsWritten))
                {
                    writer.WriteLine(span.Slice(0, charsWritten));
                }
            }
        }

        public void Sort(string inputFilePath, string outputFilePath, string mergedSeriesFileName)
        {
            logger.LogInformation("Початок сортування природнім злиттям");

            SplitIntoSeries(inputFilePath, fileB, fileC);

            while (!IsFileEmpty(fileB) && !IsFileEmpty(fileC))
            {
                MergeSeries(fileB, fileC, mergedSeriesFileName);
                SplitIntoSeries(mergedSeriesFileName, fileB, fileC);
                File.Delete(mergedSeriesFileName);
            }

            FinalizeSorting(outputFilePath);

            logger.LogInformation("Сортування завершено. Отриманий файл: {FilePath}", outputFilePath);
        }

        private void FinalizeSorting(string outputFilePath)
        {
            var nonEmptyRunFile = !IsFileEmpty(fileB) ? fileB : fileC;
            File.Copy(nonEmptyRunFile, outputFilePath, overwrite: true);

            File.Delete(fileB);
            File.Delete(fileC);
        }

        private void SplitIntoSeries(string inputFilePath, string outputFilePath1, string outputFilePath2)
        {
            logger.LogInformation("Розбиття файлу на серії: {FilePath}", inputFilePath);

            using var readerMergedSeries = new StreamReader(inputFilePath);
            using var writerB = new StreamWriter(outputFilePath1);
            using var writerC = new StreamWriter(outputFilePath2);

            StreamWriter currentWriter = writerB;
            int? previousValue = null;
            string? line;
            bool isFirstElement = true;

            while ((line = readerMergedSeries.ReadLine()) != null)
            {
                ProcessSeriesLine(line, ref currentWriter, ref previousValue, ref isFirstElement, writerB, writerC);
            }

            currentWriter.WriteLine(separator);
        }

        private void ProcessSeriesLine(string line, ref StreamWriter currentWriter, ref int? previousValue, ref bool isFirstElement, StreamWriter writerB, StreamWriter writerC)
        {
            if (string.IsNullOrWhiteSpace(line) || line == separator)
            {
                logger.LogDebug("Пропуск порожнього рядка або роздільника: {Line}", line);
                return;
            }

            if (!int.TryParse(line, out int value))
            {
                logger.LogDebug("Некоректне значення. Рядок: \"{Line}\"", line);
                return;
            }

            if (isFirstElement || value >= previousValue)
            {
                currentWriter.WriteLine(value);
            }
            else
            {
                currentWriter.WriteLine(separator);
                currentWriter = SwitchWriter(currentWriter, writerB, writerC);
                currentWriter.WriteLine(value);
            }

            previousValue = value;
            isFirstElement = false;
        }

        private StreamWriter SwitchWriter(StreamWriter currentWriter, StreamWriter writerB, StreamWriter writerC)
        {
            return currentWriter == writerB ? writerC : writerB;
        }

        private void MergeSeries(string inputFilePath1, string inputFilePath2, string outputFilePath)
        {
            logger.LogInformation("Злиття серій з файлів: {File1}, {File2}", inputFilePath1, inputFilePath2);

            using var readerB = new StreamReader(inputFilePath1);
            using var readerC = new StreamReader(inputFilePath2);
            using var writerMergedSeries = new StreamWriter(outputFilePath);

            MergeSeriesData(readerB, readerC, writerMergedSeries);
        }

        private void MergeSeriesData(StreamReader readerB, StreamReader readerC, StreamWriter writerMergedSeries)
        {
            string? line1 = ReadNextValueOrSeparator(readerB, out bool endOfFile1);
            string? line2 = ReadNextValueOrSeparator(readerC, out bool endOfFile2);

            while (!endOfFile1 || !endOfFile2)
            {
                if (line1 == null)
                {
                    WriteRemainingData(readerC, writerMergedSeries, ref line1, out endOfFile1, out line2, out endOfFile2, readerB);
                }
                else if (line2 == null)
                {
                    WriteRemainingData(readerB, writerMergedSeries, ref line2, out endOfFile2, out line1, out endOfFile1, readerC);
                }
                else
                {
                    MergeLines(writerMergedSeries, ref line1, ref line2, readerB, readerC, ref endOfFile1, ref endOfFile2);
                }
            }

            WriteUnmergedData(readerB, writerMergedSeries);
            WriteUnmergedData(readerC, writerMergedSeries);
        }

        private void MergeLines(StreamWriter writerMergedSeries, ref string? line1, ref string? line2, StreamReader readerB, StreamReader readerC, ref bool endOfFile1, ref bool endOfFile2)
        {
            if (int.Parse(line1) <= int.Parse(line2))
            {
                writerMergedSeries.WriteLine(line1);
                line1 = ReadNextValueOrSeparator(readerB, out endOfFile1);
            }
            else
            {
                writerMergedSeries.WriteLine(line2);
                line2 = ReadNextValueOrSeparator(readerC, out endOfFile2);
            }
        }

        private void WriteRemainingData(
            StreamReader readerCurrent,
            StreamWriter writerMergedSeries,
            ref string? lineCurrent,
            out bool endOfFileCurrent,
            out string? lineOther,
            out bool endOfFileOther,
            StreamReader readerOther)
        {
            writerMergedSeries.WriteLine(lineCurrent);
            WriteUnmergedData(readerCurrent, writerMergedSeries);

            lineCurrent = ReadNextValueOrSeparator(readerOther, out endOfFileOther);
            lineOther = ReadNextValueOrSeparator(readerCurrent, out endOfFileCurrent);
        }


        private string? ReadNextValueOrSeparator(StreamReader reader, out bool endOfFile)
        {
            string? line = reader.ReadLine();
            endOfFile = line == null;

            if (string.IsNullOrEmpty(line) || line == separator)
            {
                return null;
            }

            return line;
        }

        private void WriteUnmergedData(StreamReader reader, StreamWriter writer)
        {
            string? line;
            var buffer = new StringBuilder();

            while ((line = reader.ReadLine()) != null)
            {
                if (line == separator)
                {
                    break;
                }

                buffer.AppendLine(line);

                if (buffer.Length >= 8192)
                {
                    writer.Write(buffer.ToString());
                    buffer.Clear();
                }
            }

            if (buffer.Length > 0)
            {
                writer.Write(buffer.ToString());
            }
        }

        private static bool IsFileEmpty(string filePath)
        {
            return new FileInfo(filePath).Length == 0;
        }
    }
}
