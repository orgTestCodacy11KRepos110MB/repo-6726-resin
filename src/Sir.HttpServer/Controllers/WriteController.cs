﻿using System;
using System.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Sir.HttpServer.Controllers
{
    [Route("write")]
    public class WriteController : Controller
    {
        private readonly IHttpWriter _writer;
        private readonly IModel<string> _model;
        private readonly ILogger<WriteController> _logger;
        private readonly IConfigurationProvider _config;
        private readonly IIndexReadWriteStrategy _indexStrategy;

        public WriteController(
            IHttpWriter writer,
            IModel<string> tokenizer,
            IIndexReadWriteStrategy indexStrategy,
            ILogger<WriteController> logger,
            IConfigurationProvider config)
        {
            _writer = writer;
            _model = tokenizer;
            _logger = logger;
            _config = config;
            _indexStrategy = indexStrategy;
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public IActionResult Post(string accessToken)
        {
            if (!IsValidToken(accessToken))
            {
                return StatusCode((int)HttpStatusCode.MethodNotAllowed);
            }

            if (string.IsNullOrWhiteSpace(Request.ContentType))
            {
                throw new NotSupportedException();
            }

            try
            {
                _writer.Write(Request, _model, _indexStrategy);

                return Ok();
            }
            catch (Exception ew)
            {
                _logger.LogError(ew.ToString());

                return Problem(detail:ew.ToString());
            }
        }

        private bool IsValidToken(string accessToken)
        {
            if (string.IsNullOrWhiteSpace(accessToken))
                return false;

            return _config.Get("admin_password").Equals(accessToken);
        }
    }
}