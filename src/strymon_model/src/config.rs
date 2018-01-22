pub mod job {
    use std::env;
    use std::num;

    use QueryId;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Process {
        /// Job this process belongs to
        pub job_id: QueryId,
        /// Index of this process (worker group)
        pub index: usize,
        /// Addresses of all worker groups of this same job
        pub addrs: Vec<String>,
        /// Number of thread this process hosts
        pub threads: usize,
        /// Address of the Strymon coordinator
        pub coord: String,
        /// Externally reachable hostname
        pub hostname: String,
    }

    const JOB_ID: &'static str = "STRYMON_JOB_CONF_ID";
    const PROCESS_INDEX: &'static str = "STRYMON_JOB_CONF_PROCESS_INDEX";
    const PROCESS_ADDRS: &'static str = "STRYMON_JOB_CONF_PROCESS_HOSTS";
    const PROCESS_THREADS: &'static str = "STRYMON_JOB_CONF_PROCESS_THREADS";
    const COORD: &'static str = "STRYMON_JOB_CONF_COORDINATOR";
    const HOSTNAME: &'static str = "STRYMON_JOB_CONF_HOSTNAME";

    impl Process {
        pub fn from_env() -> Result<Self, EnvError> {
            Ok(Process {
                job_id: QueryId::from(env::var(JOB_ID)?.parse::<u64>()?),
                index: env::var(PROCESS_INDEX)?.parse::<usize>()?,
                addrs: env::var(PROCESS_ADDRS)?.split('|').map(From::from).collect(),
                threads: env::var(PROCESS_THREADS)?.parse::<usize>()?,
                coord: env::var(COORD)?,
                hostname: env::var(HOSTNAME)?,
            })
        }

        fn join_addrs(&self) -> String {
            if self.addrs.is_empty() {
                String::new()
            } else {
                let cap = self.addrs.iter().map(|s| s.len()).sum::<usize>() + self.addrs.len();
                let mut joined = String::with_capacity(cap);

                let mut first = true;
                for s in &self.addrs {
                    if first {
                        first = false;
                    } else {
                        joined.push('|');
                    }
                    joined.push_str(s);
                }

                joined
            }
        }

        pub fn into_env(&self) -> Vec<(&'static str, String)> {
            vec![
                (JOB_ID, self.job_id.0.to_string()),
                (PROCESS_INDEX, self.index.to_string()),
                (PROCESS_ADDRS, self.join_addrs()),
                (PROCESS_THREADS, self.threads.to_string()),
                (COORD, self.coord.clone()),
                (HOSTNAME, self.hostname.clone()),
            ]
        }
    }

    /// Error which occurs when the job process configuration cannot be parsed
    /// from the local environment.
    #[derive(Debug)]
    pub enum EnvError {
        VarErr(env::VarError),
        IntErr(num::ParseIntError),
    }

    impl From<env::VarError> for EnvError {
        fn from(var: env::VarError) -> Self {
            EnvError::VarErr(var)
        }
    }

    impl From<num::ParseIntError> for EnvError {
        fn from(int: num::ParseIntError) -> Self {
            EnvError::IntErr(int)
        }
    }
}
