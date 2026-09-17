# Live matrix — 3/5 passed

| result | case | detail |
|---|---|---|
| FAIL | openai/gpt-oss-20b:free chat reasoning_effort low | HTTP 424: {"error":{"code":"model_error_exception","message":"This model is unavailable for free. The paid version is available now - use this slug instead: openai/gpt-oss-20b","original_status_code":404,"param":null,"resource_name":"openai/gpt-oss-20b:free","type":"model_error"}} |
| FAIL | openai/gpt-oss-20b:free chat stream [upstream fault] | vendor failed mid-stream; terminal frame {"error": {"code": "model_error_exception", "message": "This model is unavailable for free. The paid version is available now - use this slug instead: openai/gpt-oss-20b", "original_status_code": 404,; billed p/c/t=28/108/136 estimated=False |
| PASS | openai/gpt-6-astra chat effort xhigh | wire={"prompt_tokens":14,"completion_tokens":35,"total_tokens":49,"completion_tokens_details":{"reasoning_tokens":27}} ledger p/c/t/cost=(14, 35, 49, 1890) oracle=(14, 35, 49, 1890) text='Yes.' |
| PASS | openai/gpt-6-astra chat effort none clamps, the route refuses it | wire={"prompt_tokens":14,"completion_tokens":6,"total_tokens":20} ledger p/c/t/cost=(14, 6, 20, 440) oracle=(14, 6, 20, 440) text='Yes.' |
| PASS | openai/gpt-5.6-luna chat max and knobs ride through | wire={"prompt_tokens":14,"completion_tokens":6,"total_tokens":20} ledger p/c/t/cost=(14, 6, 20, 9) oracle=(14, 6, 20, 9) text='Yes.' |
