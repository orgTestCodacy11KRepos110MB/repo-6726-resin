using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Sir.Strings;

namespace Sir.HttpServer.Controllers
{
    public abstract class UIController : Controller
    {
        private IConfigurationProvider config;

        protected IConfigurationProvider Config { get; }
        protected SessionFactory Database { get; }

        public UIController(IConfigurationProvider config, SessionFactory database)
        {
            Config = config;
            Database = database;
        }

        protected UIController(IConfigurationProvider config)
        {
            this.config = config;
        }

        public override void OnActionExecuted(ActionExecutedContext context)
        {        
            base.OnActionExecuted(context);
        }
    }
}