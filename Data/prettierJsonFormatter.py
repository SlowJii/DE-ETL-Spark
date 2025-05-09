import json

# JSON gốc (dạng chuỗi)
json_string = '''{"id":"2615567678","type":"IssuesEvent","actor":{"id":9614759,"login":"GoogleCodeExporter","gravatar_id":"","url":"https://api.github.com/users/GoogleCodeExporter","avatar_url":"https://avatars.githubusercontent.com/u/9614759?"},"repo":{"id":31501832,"name":"chrsmith/html5rocks","url":"https://api.github.com/repos/chrsmith/html5rocks"},"payload":{"action":"opened","issue":{"url":"https://api.github.com/repos/chrsmith/html5rocks/issues/349","labels_url":"https://api.github.com/repos/chrsmith/html5rocks/issues/349/labels{/name}","comments_url":"https://api.github.com/repos/chrsmith/html5rocks/issues/349/comments","events_url":"https://api.github.com/repos/chrsmith/html5rocks/issues/349/events","html_url":"https://github.com/chrsmith/html5rocks/issues/349","id":59405714,"number":349,"title":"have html5 badge for author page","user":{"login":"GoogleCodeExporter","id":9614759,"avatar_url":"https://avatars.githubusercontent.com/u/9614759?v=3","gravatar_id":"","url":"https://api.github.com/users/GoogleCodeExporter","html_url":"https://github.com/GoogleCodeExporter","followers_url":"https://api.github.com/users/GoogleCodeExporter/followers","following_url":"https://api.github.com/users/GoogleCodeExporter/following{/other_user}","gists_url":"https://api.github.com/users/GoogleCodeExporter/gists{/gist_id}","starred_url":"https://api.github.com/users/GoogleCodeExporter/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/GoogleCodeExporter/subscriptions","organizations_url":"https://api.github.com/users/GoogleCodeExporter/orgs","repos_url":"https://api.github.com/users/GoogleCodeExporter/repos","events_url":"https://api.github.com/users/GoogleCodeExporter/events{/privacy}","received_events_url":"https://api.github.com/users/GoogleCodeExporter/received_events","type":"User","site_admin":false},"labels":[{"url":"https://api.github.com/repos/chrsmith/html5rocks/labels/auto-migrated","name":"auto-migrated","color":"ededed"},{"url":"https://api.github.com/repos/chrsmith/html5rocks/labels/Priority-P2","name":"Priority-P2","color":"ededed"},{"url":"https://api.github.com/repos/chrsmith/html5rocks/labels/Type-Bug","name":"Type-Bug","color":"ededed"}],"state":"open","locked":false,"assignee":null,"milestone":null,"comments":0,"created_at":"2015-03-01T17:00:00Z","updated_at":"2015-03-01T17:00:00Z","closed_at":null,"body":"\\nWhat steps will reproduce the problem?\\n1.\\n2.\\n3.\\n\\nWhat is the expected output? What do you see instead?\\n\\n\\nPlease use labels and text to provide additional information.\\n\\n\\n\\nOriginal issue reported on code.google.com by `v...@google.com` on 16 Dec 2010 at 11:33"}},"public":true,"created_at":"2015-03-01T17:00:00Z"}'''

# Chuyển chuỗi JSON thành đối tượng Python
try:
    json_data = json.loads(json_string)

    # Format JSON với indent=4 để dễ đọc
    formatted_json = json.dumps(json_data, indent=4, ensure_ascii=False)

    # In ra JSON đã format
    print(formatted_json)

    # Lưu JSON vào file (tùy chọn)
    with open('formatted_json.json', 'w', encoding='utf-8') as f:
        f.write(formatted_json)

except json.JSONDecodeError as e:
    print(f"Lỗi khi phân tích JSON: {e}")