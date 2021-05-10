RFC/Design Process
==================

A lot of changes are minor in nature and can be done in the regular Issue + PR
workflow, but some changes are substantial and have far reaching consequences
that might affect users or dependent components in a major way, or they are
big new features that need a lot of design work.

For these cases, the RFC (Request For Comments) process provides a persistent
and controlled path for new features and changes to enter the codebase.

They also give users and other third parties a chance to see and contribute
to the design of these features.

When to follow this process
---------------------------

This process should be followed in case of substantial changes to the codebase.
What constitues a substantial change is not set in stone, but may include:

- adding new service endpoints/changing service endpoints in way that break
  downstream code
- Adding new groups of cli commands, adding new cli commands that have a big
  impact on user interaction with renku
- significant changes to the json-ld metadata that go beyond e.g. adding a
  simple field

What the process is
-------------------

- Copy the `rfc-template.md` file and name it like `0000-my-feature.md`
  (with an incrementing number and appropriate title)
- Fill in the copied template with design details, taking care to address
  the motivations, impact and design criteria/limitations
- Submit a merge request for the RFC
- After the merge request is merged, open a github discussion referencing
  the RFC with a short introduction. Announce the discussion on discourse
  in case of bigger changes. This allows the community to review the RFC
  and give feedback
- A merged RFC can be modified, accepted or rejected.

Once an RFC has been accepted, it can be broken up into issues and
implemented as usual.

RFCs are not meant to be set in stone and can be subject to change.
As with any large undertaking, assumptions can turn out to be false,
better solutions may present themselves or they may turn out to not be
feasible after all. So RFCs can change after being accepted. But care should
be taken to make them as robust as possible.
