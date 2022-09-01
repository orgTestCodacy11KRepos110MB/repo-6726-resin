﻿using Microsoft.AspNetCore.Http;
using System.Threading.Tasks;

namespace Sir.Strings
{
    public interface IQueryFormatter<T>
    {
        Task<T> Format(HttpRequest request, IModel<T> model);
    }
}