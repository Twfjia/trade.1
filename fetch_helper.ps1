# 持久取数通道：从 stdin 逐行读取 URL，返回 "OK <base64>" 或 "ERR <msg>"
$stdin = [Console]::In
while (($line = $stdin.ReadLine()) -ne $null) {
  if ($line.Length -eq 0) { continue }
  try {
    $r = Invoke-WebRequest -Uri $line -UseBasicParsing -TimeoutSec 25 -Headers @{ 'User-Agent' = 'Mozilla/5.0'; 'Referer' = 'https://quote.eastmoney.com/' }
    $b = [Convert]::ToBase64String($r.RawContentStream.ToArray())
    [Console]::Out.WriteLine("OK " + $b)
  } catch {
    [Console]::Out.WriteLine("ERR " + $_.Exception.Message)
  }
}
