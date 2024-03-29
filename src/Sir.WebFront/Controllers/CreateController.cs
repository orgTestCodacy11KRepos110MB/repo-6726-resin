﻿using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Sir.HttpServer.Controllers
{
    public class CreateController : UIController
    {
        public CreateController(IConfigurationProvider config, SessionFactory database) : base(config, database)
        {
        }

        [HttpGet("/addurl")]
        public ActionResult AddUrl(string url, string[] urls, string scope, string returnUrl, string queryId)
        {
            var urlList = urls.Where(s => !string.IsNullOrWhiteSpace(s)).Select(s => new Uri(s).ToString()).ToList();

            if (url is null)
            {
                var query = $"?urls={string.Join("&urls=", urlList.Select(s => Uri.EscapeDataString(s)))}&errorMessage=Cannot add empty URL.";

                if (!string.IsNullOrWhiteSpace(queryId))
                    query += $"&queryId={queryId}";

                var retUri = new Uri(returnUrl + query, UriKind.Relative);

                return Redirect(retUri.ToString());
            }

            if (returnUrl is null)
            {
                return BadRequest();
            }

            Uri uri;

            try
            {
                uri = new Uri(url);

                if (uri.Scheme != "https")
                    throw new Exception("Scheme was http. Scheme must be https.");
            }
            catch (Exception ex)
            {
                ViewData["errorMessage"] = ex.Message;

                var query = $"?urls={string.Join("&urls=", urlList.Select(s => Uri.EscapeDataString(s)))}";

                var retUri = new Uri(returnUrl + query, UriKind.Relative);

                return Redirect(retUri.ToString());
            }

            if (scope == "page")
            {
                urlList.Add(url.Replace("https://", "page://"));
            }
            else
            {
                urlList.Add(url.Replace("https://", "site://"));
            }

            var queryString = $"?urls={string.Join("&urls=", urlList.Select(s => Uri.EscapeDataString(s)))}";

            if (!string.IsNullOrWhiteSpace(queryId))
                queryString += $"&queryId={queryId}";

            var returnUri = new Uri(returnUrl + queryString, UriKind.Relative);

            return Redirect(returnUri.ToString());
        }

        [HttpGet("/deleteurl")]
        public ActionResult DeleteUrl(string url, string returnUrl, string queryId)
        {
            if (url is null)
            {
                throw new ArgumentNullException(nameof(url));
            }

            if (returnUrl is null)
            {
                throw new ArgumentNullException(nameof(returnUrl));
            }

            var urlList = Request.Query["urls"].ToList();

            urlList.Remove(url);

            var queryString = $"?urls={string.Join("&urls=", urlList.Select(s => Uri.EscapeDataString(s)))}";

            if (!string.IsNullOrWhiteSpace(queryId))
                queryString += $"&queryId={queryId}";

            var returnUri = new Uri(returnUrl + queryString, UriKind.Relative);

            return Redirect(returnUri.ToString());
        }

        [HttpPost("/createindex")]
        public ActionResult CreateIndex(string[] urls, string agree, string returnUrl)
        {
            if (urls.Length == 0 || urls[0] == null)
            {
                var queryString = $"?errorMessage={StringUtil.Base64Encode("URL list is empty")}.";
                var returnUri = new Uri(returnUrl + queryString, UriKind.Relative);

                return Redirect(returnUri.ToString());
            }

            if (agree != "yes")
            {
                var queryString = $"?urls={string.Join("&urls=", urls.Select(s => Uri.EscapeDataString(s)))}&errorMessage={StringUtil.Base64Encode("It is required that you read and agree to the terms.")}";
                var returnUri = new Uri(returnUrl + queryString, UriKind.Relative);

                return Redirect(returnUri.ToString());
            }

            var uris = new List<(Uri uri, string scope)>();

            //validate that all entries are parsable into Uris
            try
            {
                foreach (var url in urls)
                {
                    uris.Add((new Uri(url.Replace("page://", "https://").Replace("site://", "https://")), url.StartsWith("page://") ? "page" : "site"));
                }
            }
            catch (Exception ex)
            {
                var queryString = $"?urls={string.Join("&urls=", urls.Select(s => Uri.EscapeDataString(s)))}&errorMessage=URL list is not valid. {ex}";
                var returnUri = new Uri(returnUrl + queryString, UriKind.Relative);

                return Redirect(returnUri.ToString());
            }

            var queryId = Guid.NewGuid().ToString();
            var userDirectory = Path.Combine(Config.Get("user_dir"), queryId);

            try
            {
                if (Directory.Exists(userDirectory))
                {
                    return new ConflictResult();
                }

                Directory.CreateDirectory(userDirectory);

                var urlCollectionId = "url".ToHash();
                var documents = new List<Document>();

                foreach (var uri in uris)
                {
                    documents.Add(new Document(new Field[]
                    {
                    new Field("url", uri.uri.ToString()),
                    new Field("host", uri.uri.Host),
                    new Field("scope", uri.scope),
                    new Field("verified", false)
                    }));
                }

                Database.Store(
                    userDirectory,
                    urlCollectionId,
                    documents);

                return RedirectToAction("Index", "Search", new { queryId });
            }
            catch
            {
                return new ConflictResult();
            }
        }
    }
}
