### Pull Request Info

Branch protection is enabled on the default branch of the repo (typically main). This is so that code reviews are
performed by a peer before code is merged.

In case of a time sensitive PR, it is possible to auto-approve it by adding a comment containing `/auto-approve`.
A review is still required in such a case but can be performed at a later stage. Auto approval triggers a notification
to the #emergency-code-change Slack channel so that someone gets aware of the change and can go and review it as soon
as possible.

Details on handling emergency pull requests can be found [on Confluence](https://logchat.atlassian.net/wiki/spaces/Engineerin/pages/98533377/Pull+Requests).
