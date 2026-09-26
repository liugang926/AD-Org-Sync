import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    os.environ["AD_ORG_SYNC_DATA_DIR"] = tmp
    os.environ["DJANGO_SETTINGS_MODULE"] = "sync_app.settings"
    os.environ["LDAP_BASE_DN"] = "DC=example,DC=com"
    os.environ["DINGTALK_CORP_ID"] = "synthetic-company"
    os.environ["DINGTALK_APP_KEY"] = "synthetic-app"
    import django
    django.setup()
    from django.core.management import call_command

    from sync_app.models import Binding, Configuration, Job, Person
    from sync_app.synchronization import plan
    from tests.fakes import Directory, Source, account, user

    call_command("migrate", verbosity=0)
    config = Configuration.current()
    config.root_ou = "OU=Company,DC=example,DC=com"
    config.attributes = ["displayName"]
    config.disable_missing = True
    config.save()
    users = [user("alice", "1001"), user("bob", "1002"), user("charlie", "1003"), user("dave", "1004"), user("eric", "1006")]
    for row, name in zip(users, ("Alice", "Bob", "Charlie Updated", "Dave", "Eric")):
        row["name"] = name
        row["email"] = row["source_id"] + "@example.com"
    users[3]["departments"] = ["2"]
    users[3]["primary_department"] = "2"
    source = Source(users)
    source.collect = lambda root: (users, [{"id": "1", "name": "Company", "parent": "0"}, {"id": "2", "name": "Engineering", "parent": "1"}])
    existing = account("1001", "1001", "11111111-1111-1111-1111-111111111111")
    existing["dn"] = "CN=Alice,OU=Company,DC=example,DC=com"
    existing["email"] = "alice@example.com"
    existing["attrs"] = {"displayName": "Alice"}
    charlie = account("1003", "charlie1003", "33333333-3333-3333-3333-333333333333")
    charlie["dn"] = "CN=Charlie,OU=Company,DC=example,DC=com"
    charlie["attrs"] = {"displayName": "Charlie"}
    dave = account("1004", "dave1004", "44444444-4444-4444-4444-444444444444")
    dave["dn"] = "CN=Dave,OU=Company,DC=example,DC=com"
    dave["attrs"] = {"displayName": "Dave"}
    gone = account("1005", "gone1005", "55555555-5555-5555-5555-555555555555")
    gone["dn"] = "CN=Gone,OU=Company,DC=example,DC=com"
    eric_one = account("1006", "1006", "66666666-6666-6666-6666-666666666666")
    eric_one["dn"] = "CN=EricOne,OU=Company,DC=example,DC=com"
    eric_two = account("1006", "eric1006", "77777777-7777-7777-7777-777777777777")
    eric_two["dn"] = "CN=EricTwo,OU=Company,DC=example,DC=com"
    directory = Directory([existing, charlie, dave, gone, eric_one, eric_two])
    directory.ous = {config.root_ou.casefold(): "22222222-2222-2222-2222-222222222222"}
    for uid, target in (("charlie", charlie), ("dave", dave), ("gone", gone)):
        person = Person.objects.create(source_id=uid, name=uid)
        Binding.objects.create(person=person, object_guid=target["guid"], username=target["username"])
    result = plan(Job.objects.create(), source, directory)
    actions = {op["source_id"]: op["action"] for op in result["operations"]}
    assert actions == {"alice": "bind", "bob": "create", "charlie": "update", "dave": "move", "eric": "conflict", "gone": "disable"}
    assert result["high_risk"] is True
    assert directory.created == 0 and directory.disabled == []
    assert Binding.objects.count() == 3
    print(json.dumps({
        "operations": [{"source_id": op["source_id"], "action": op["action"], "username": op.get("username"), "ou": op.get("ou"), "changes": op.get("changes")} for op in result["operations"]],
        "departments": [{"source_id": op["source_id"], "action": op["action"], "dn": op["dn"], "guid": op["guid"]} for op in result["departments"]],
        "high_risk": result["high_risk"],
    }, ensure_ascii=False, indent=2))
    from django.db import connections
    connections.close_all()
