def build_html_body(signals: list, now_utc: str) -> str:
    rows = []
    for s in signals:
        tv = _tv_url(s["inst_id"], s["tf"])
        rows.append(f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">
            <a href="{tv}" style="color:#0366d6;text-decoration:none;font-weight:600;">
              {s['inst_id']}
            </a>
          </td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">
            {s['tf']}
          </td>
        </tr>""")

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f6f8fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#24292e;">
  <div style="max-width:600px;margin:20px auto;background:#fff;border-radius:8px;padding:24px;box-shadow:0 1px 3px rgba(0,0,0,0.08);">
    <h2 style="margin:0 0 4px 0;font-size:18px;">LONG [EMA+MACD]</h2>
    <div style="color:#586069;font-size:13px;margin-bottom:18px;">{now_utc} · всего: {len(signals)}</div>
    <table style="border-collapse:collapse;width:100%;font-size:14px;">
      <thead>
        <tr style="background:#f6f8fa;text-align:left;">
          <th style="padding:10px 12px;">Инструмент</th>
          <th style="padding:10px 12px;text-align:center;">ТФ</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
    <div style="margin-top:20px;padding-top:14px;border-top:1px solid #eee;color:#586069;font-size:12px;">
      EMA+MACD Alert Bot (OKX / GitHub Actions). Ссылки ведут в TradingView.
    </div>
  </div>
</body>
</html>"""
