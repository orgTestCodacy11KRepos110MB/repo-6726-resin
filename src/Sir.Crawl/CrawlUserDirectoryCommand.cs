using HtmlAgilityPack;
using Microsoft.Extensions.Logging;
using Sir.Search;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Sir.Crawl
{
    /// <summary>
    /// Example: crawluserdirectory --dataDirectory c:\data\resin --userDirectory c:\data\resin\user
    /// </summary>
    public class CrawlUserDirectoryCommand : ICommand
    {
        private readonly HashSet<string> _select = new HashSet<string> { "url", "scope", "verified" };
        private readonly IModel<string> _model = new BagOfCharsModel();

        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dataDirectory = args["dataDirectory"];
            var rootUserDirectory = args["userDirectory"];
            var maxNoRequestsPerSession = args.ContainsKey("maxNoRequestsPerSession") ? int.Parse(args["maxNoRequestsPerSession"]) : 10;
            var minIdleTime = args.ContainsKey("minIdleTime") ? int.Parse(args["minIdleTime"]) : 500;
            var refresh = args.ContainsKey("refresh");

            var urlCollectionId = "url".ToHash();
            var htmlClient = new HtmlWeb();
            var model = new BagOfCharsModel();

            htmlClient.UserAgent = "Crawlcrawler (+https://crawlcrawler.com)";

#if DEBUG
            htmlClient.UserAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36";
#endif
            using (var database = new SessionFactory(logger))
            using (var dataSearchSession = new SearchSession(dataDirectory, database, model, logger))
            {
                foreach (var userDirectory in Directory.EnumerateDirectories(rootUserDirectory))
                {
                    var verifiedKeyId = database.GetKeyId(userDirectory, urlCollectionId, "verified".ToHash());

                    foreach (var url in database.Select(userDirectory, urlCollectionId, _select))
                    {
                        bool verified = false;
                        Uri uri = null;
                        string scope = null;

                        foreach (var field in url.Fields)
                        {
                            if (field.Name == "url")
                                uri = new Uri((string)field.Value);
                            else if (field.Name == "scope")
                                scope = (string)field.Value;
                            else if (field.Name == "verified")
                                verified = (bool)field.Value;
                        }

                        if (verified && !refresh)
                        {
                            continue;
                        }

                        var collectionId = uri.Host.ToHash();

                        if (database.DocumentExists(dataDirectory, uri.Host, "url", uri.ToString(), _model, dataSearchSession))
                        {
                            database.Update(userDirectory, urlCollectionId, url.Id, verifiedKeyId, true);
                            continue;
                        }

                        var siteWide = scope == "site";

                        try
                        {
                            var time = Stopwatch.StartNew();
                            var idleTime = Stopwatch.StartNew();
                            var result = Crawl(uri, htmlClient, siteWide, logger);

                            // set document to "verified" even if response was null
                            database.Update(userDirectory, urlCollectionId, url.Id, verifiedKeyId, true);

                            if (result != null)
                            {
                                database.StoreDataAndPersistIndex(dataDirectory, collectionId, result.Document, _model);
                                
                                int crawlCount = 1;

                                foreach (var link in result.Links)
                                {
                                    if (crawlCount == maxNoRequestsPerSession)
                                        break;

                                    if (database.DocumentExists(dataDirectory, link.Host, "url", link.ToString(), _model, dataSearchSession))
                                    {
                                        continue;
                                    }

                                    while (idleTime.ElapsedMilliseconds < minIdleTime)
                                    {
                                        Thread.Sleep(200);
                                    }

                                    idleTime.Restart();

                                    var r = Crawl(link, htmlClient, siteWide: false, logger);

                                    if (r != null)
                                    {
                                        database.StoreDataAndPersistIndex(dataDirectory, collectionId, r.Document, _model);
                                    }

                                    crawlCount++;
                                }

                                logger.LogInformation($"crawling {crawlCount} resources from {uri.Host} and storing the responses took {time.Elapsed}.");
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Crawl error");
                        }
                    }
                }
            }
        }

        private class CrawlResult
        {
            public Document Document { get; set; }
            public HashSet<Uri> Links { get; set; }
        }

        private CrawlResult Crawl(Uri uri, HtmlWeb htmlClient, bool siteWide, ILogger logger)
        {
            try
            {
                var doc = htmlClient.Load(uri);
                var titleNode = doc.DocumentNode.Descendants("title").FirstOrDefault();

                if (titleNode == null || string.IsNullOrWhiteSpace(titleNode.InnerText))
                {
                    logger.LogInformation($"unsuccessful crawl of {uri}");

                    return null;
                }

                logger.LogInformation($"crawled {uri}");

                var title = titleNode.InnerText;
                var sb = new StringBuilder();

                foreach (var node in doc.DocumentNode.DescendantsAndSelf())
                {
                    if (!node.HasChildNodes && node.ParentNode.Name != "script" && node.ParentNode.Name != "style")
                    {
                        string innerText = node.InnerText;

                        if (!string.IsNullOrEmpty(innerText))
                            sb.AppendLine(innerText.Trim());
                    }
                }

                var text = sb.ToString().Trim();

                var document = new Document(new Field[]
                {
                new Field("title", title),
                new Field("text", text.ToString()),
                new Field("url", uri.ToString()),
                new Field("last_crawl_date", DateTime.Now)
                });

                var links = new HashSet<Uri>();

                if (siteWide)
                {
                    var root = $"{uri.Scheme}://{uri.Host}{uri.PathAndQuery}";
                    var nodes = doc.DocumentNode.SelectNodes("//a[@href]");

                    if (nodes == null)
                    {
                        logger.LogInformation($"unable to parse {uri}");

                        return null;
                    }

                    foreach (HtmlNode link in nodes)
                    {
                        var href = link.Attributes["href"].Value;
                        Uri linkUri = null;

                        try
                        {
                            if (href.StartsWith('/'))
                            {
                                linkUri = new Uri($"{root}{href.Substring(1)}");
                            }
                            else if (href.StartsWith("http"))
                            {
                                linkUri = new Uri(href);
                            }
                            else if (href.Contains('/'))
                            {
                                linkUri = new Uri($"{root}{uri.PathAndQuery}{href}");
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Crawl error occurred while iterating <a> tags.");
                        }

                        if (linkUri != null)
                        {
                            if (linkUri.Host == uri.Host)
                            {
                                links.Add(linkUri);
                            }
                        }
                    }
                }

                return new CrawlResult { Document = document, Links = links };
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"error while crawling url {uri} (sitewide: {siteWide})");

                return null;
            }
        }
    }
}
