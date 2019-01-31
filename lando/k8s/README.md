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

Build the calrissian image
```
oc create -f https://raw.githubusercontent.com/Duke-GCB/calrissian/master/openshift/BuildConfig.yaml
oc create role pod-manager-role --verb=create,delete,list,watch --resource=pods
oc create rolebinding pod-manager-default-binding --role=pod-manager-role --serviceaccount=lando-job-runner:default
```

Build the lando-util image
```
oc create -f https://raw.githubusercontent.com/Duke-GCB/lando-util/master/openshift/BuildConfig.yml
```

If desired create a persistent volume for holding system data.

Create a config file.
TODO

In one terminal run the k8s watcher
```

```

In another terminal run k8s lando
```

```
