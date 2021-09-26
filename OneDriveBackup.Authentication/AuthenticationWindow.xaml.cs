using System;
using System.Windows;

namespace OneDriveBackup.Authentication;

public partial class AuthenticationWindow : Window
{
    private readonly Uri _loginUrl;

    public AuthenticationWindow(Uri loginUrl)
    {
        InitializeComponent();
        Loaded += AuthenticationWindow_Loaded;
        _loginUrl = loginUrl;
    }

    private void AuthenticationWindow_Loaded(object sender, RoutedEventArgs e)
    {
        webView.Source = _loginUrl;
    }

    public string? AuthorizationCode { get; private set; }

    private void WebView_NavigationStarting(object sender, Microsoft.Web.WebView2.Core.CoreWebView2NavigationStartingEventArgs e)
    {
        var url = new Uri(e.Uri);

        var query = url.Query;
        if (query == null)
            return;

        query = query[1..]; // remove starting '?'
        var parts = query.Split('&');
        foreach (var part in parts)
        {
            const string codeToken = "code=";
            if (!part.StartsWith(codeToken, StringComparison.OrdinalIgnoreCase))
                continue;

            var value = Uri.UnescapeDataString(part[codeToken.Length..]);
            AuthorizationCode = value;
            Close();
        }
    }
}
