You are evaluating a CodeRabbit PR review finding.

Do NOT blindly implement the suggestion. 
Reject CodeRabbit suggestions that introduce refactors not required to fix the verified issue.

Your job is to:
1) locate the relevant code
2) verify the issue actually exists
3) apply the smallest fix only if necessary

--------------------------------------------------

CODE RABBIT FINDING
-------------------

[PASTE THE FULL CODERABBIT COMMENT HERE]

--------------------------------------------------

TASK CONTEXT
------------

Active task:
[task id / title]

Constraints:
- Only fix issues that are real
- Only fix issues within the current task scope unless it is a clear correctness bug
- Prefer the smallest deterministic change
- Avoid refactors unless required
- Maintain ≥90% test coverage for changed production packages
- Do not change production behavior unless required to fix the issue

--------------------------------------------------

REPOSITORY INVESTIGATION
------------------------

1. Locate the file referenced in the CodeRabbit finding.
2. Navigate to the approximate line numbers mentioned.
3. Inspect surrounding code to confirm whether the issue exists.
4. Verify whether the code behavior matches CodeRabbit’s claim.

--------------------------------------------------

EVALUATION STEPS
----------------

1. Confirm whether the issue actually exists.
2. Determine if CodeRabbit’s suggested fix is:
   - correct
   - partially correct
   - incorrect
3. Determine severity:
   - none
   - low (style)
   - medium (edge-case correctness)
   - high (real bug / data corruption)

4. Determine scope:
   - in scope
   - partially in scope
   - out of scope

--------------------------------------------------

DECISION OPTIONS
----------------

Choose exactly one:

ACCEPT  
→ implement minimal fix

PARTIAL  
→ issue is real but fix must be adjusted

REJECT  
→ no code change required

--------------------------------------------------

OUTPUT FORMAT
-------------

Finding Summary

Verification Result
- Issue exists: yes / no / partially
- Explanation

Severity
- none / low / medium / high

Scope Check
- in scope / partially / out of scope

Decision
- ACCEPT / PARTIAL / REJECT

Implementation Plan
- minimal change required
- or state: no change required

Updated Code
- show the modified snippet if a change is made

Test Impact
- tests to add or update

Follow-Up Notes
- optional future improvement