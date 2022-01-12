using Microsoft.AspNetCore.Mvc;

namespace Sir.HttpServer.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            return Json(new { status = "Sir.HttpServer is alive." });
        }
    }
}
