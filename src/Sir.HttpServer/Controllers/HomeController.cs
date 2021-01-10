﻿using Microsoft.AspNetCore.Mvc;
using Sir.Search;

namespace Sir.HttpServer.Controllers
{
    public class HomeController : UIController
    {
        private readonly IConfigurationProvider _config;

        public HomeController(IConfigurationProvider config, StreamFactory sessionFactory) 
            : base(config, sessionFactory)
        {
            _config = config;
        }

        public ActionResult Index()
        {
            return View();
        }

        [Route("/terms")]
        public ActionResult Terms()
        {
            return View();
        }
    }
}