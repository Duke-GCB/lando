Create a project
```
oc new-project lando-job-runner
```

Create a service account
```
oc create sa lando
```

Give this account admin priv (for now)
```
oc create rolebinding lando-binding --clusterrole=admin --serviceaccount=lando-job-runner:lando
```

Find the name of a token for this service account
```
oc describe sa lando
```

Determine the token value:
```
oc describe secret <tokename>
```
This value will need to be added to your k8s lando config file.

Create a DukeDS agent secret
Create a file containing your ddsclient config named `ddsclient.conf`.
Use this file to populate the DukeDS secret for your agent.
```
oc create secret generic ddsclient-agent --from-file=config=ddsclient.cred
```
