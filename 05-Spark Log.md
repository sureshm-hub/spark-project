# Logging:
    https://chatgpt.com/g/g-p-67bcef0cb0948191a7c057bfd5890a7b/c/6924f881-eaf0-832e-9f74-a86b20ce56a5
    EMR step  vs application vs container vs Driver /Executor Logs
    EMR Step
        ↳ YARN / Spark Application
            ↳ Containers (on nodes / pods)
                ↳ Driver logs & Executor logs
    How to debug in practice (which logs first?), When something fails on EMR:
    EMR Step:
        -   Did the step itself fail fast?
            → Check step logs: command line, exit code, immediate script errors.
    Application / Driver:
        -   Step ran but Spark failed?
            → Find the YARN/Spark application ID from the step, then:
        -   Look at driver logs for:
            Stage failures, exceptions, bad config, etc.
    Executors / Containers:
        -   Driver log says “Stage X failed due to task failures”?
            → From Spark UI, open the failed stage → failed tasks → executor logs:
        -   Look for OOMs, corrupt input, skew, etc.
        -   If a particular node/container keeps failing, go one level down:
            Node / container logs (YARN NodeManager, OS issues).

# Gotcha's:
    -   UNRESOLVED_COLUMN exception happens in analysis phase while building logical plan, Catalyst get the catalog
        from Metastore or file and when referred column is missing it throws UNRESOLVED_COLUMN exception