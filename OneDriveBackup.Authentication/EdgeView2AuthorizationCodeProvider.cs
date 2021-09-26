using System;
using System.Threading;
using Meziantou.OneDrive;

namespace OneDriveBackup.Authentication;
public sealed class EdgeView2AuthorizationCodeProvider : IAuthorizationProvider
{
    public string LoginUrl { get; set; } = "https://login.live.com/oauth20_authorize.srf";

    public string? GetAuthorizationCode(OneDriveClient client)
    {
        AuthenticationWindow? authenticationForm = null;
        var t = new Thread(o =>
        {
            string stringToEscape = (client.Scopes != null) ? string.Join(" ", client.Scopes) : string.Empty;
            authenticationForm = new AuthenticationWindow(new Uri(LoginUrl + "?client_id=" + Uri.EscapeDataString(client.ApplicationId) + "&scope=" + Uri.EscapeDataString(stringToEscape) + "&response_type=code&redirect_uri=" + Uri.EscapeDataString(client.ReturnUrl)));
            authenticationForm.ShowDialog();

        });
        t.SetApartmentState(ApartmentState.STA);
        t.Start();
        t.Join();

        return authenticationForm?.AuthorizationCode;
    }
}
