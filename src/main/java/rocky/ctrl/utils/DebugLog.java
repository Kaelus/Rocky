package rocky.ctrl.utils;

public class DebugLog {
	public static boolean VERBOSE = true;
	public static boolean DEBUG = true;
	public static boolean SUPER_TESTER_LOG = true;
	public static boolean TESTER_LOG = true;

	private String loggerID;

	public DebugLog(String name) {
		loggerID = name;
	}

	public static void log(String message) {
		if (VERBOSE) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			System.out.println("(ThreadID:" + Thread.currentThread().getId() + ") "
					+ "[" + time + " " + className + "." + methodName
					+ "():" + lineNumber + "] " + "# " + message);
		}
	}

	public static void elog(String message) {
		if (VERBOSE) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			System.err.println("(ThreadID:" + Thread.currentThread().getId() + ") "
					+ "[" + time + " " + className + "." + methodName
					+ "():" + lineNumber + "] " + "# " + message);
		}
	}

	public static void log(String message, String name) {
		if (VERBOSE) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			if (name != null) {
				System.out.println("(ThreadID:" + Thread.currentThread().getId() + ") "
						+ "[" + time + " " + className + "."
						+ methodName + "():" + lineNumber + "] " + "<=[" + name
						+ "]=> " + "# " + message);
			} else {
				System.out.println("(ThreadID:" + Thread.currentThread().getId() + ") "
						+ "[" + time + " " + className + "."
						+ methodName + "():" + lineNumber + "] " + "# " + message);
			}
		}
	}

	public static void elog(String message, String name) {
		if (VERBOSE) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			if (name != null) {
				System.err.println("(ThreadID:" + Thread.currentThread().getId() + ") "
						+ "[" + time + " " + className + "."
						+ methodName + "():" + lineNumber + "] " + "<=[" + name
						+ "]=> " + "# " + message);
			} else {
				System.err.println("(ThreadID:" + Thread.currentThread().getId() + ") "
						+ "[" + time + " " + className + "."
						+ methodName + "():" + lineNumber + "] " + "# " + message);

			}
		}
	}

	public void dlog(String message) {
		if (DEBUG) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			System.out.println("(ThreadID:" + Thread.currentThread().getId() + ") "
					+ "[" + time + " " + className + "." + methodName
					+ "():" + lineNumber + "] " + "<=[" + this.loggerID
					+ "]=> " + "# " + message);
		}
	}

	public void delog(String message) {
		if (DEBUG) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			System.err.println("(ThreadID:" + Thread.currentThread().getId() + ") "
					+ "[" + time + " " + className + "." + methodName
					+ "():" + lineNumber + "] " + "<=[" + this.loggerID
					+ "]=> " + "# " + message);
		}
	}

	public void tlog(String message) {
		if (TESTER_LOG) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			System.out.println("(ThreadID:" + Thread.currentThread().getId() + ") "
					+ "[" + time + " " + className + "." + methodName
					+ "():" + lineNumber + "] " + "<=[" + this.loggerID
					+ "]=> " + "# " + message);
		}
	}

	public void telog(String message) {
		if (TESTER_LOG) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			System.err.println("(ThreadID:" + Thread.currentThread().getId() + ") "
					+ "[" + time + " " + className + "." + methodName
					+ "():" + lineNumber + "] " + "<=[" + this.loggerID
					+ "]=> " + "# " + message);
		}
	}

	public void stlog(String message) {
		if (SUPER_TESTER_LOG) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			System.out.println("(ThreadID:" + Thread.currentThread().getId() + ") "
					+ "[" + time + " " + className + "." + methodName
					+ "():" + lineNumber + "] " + "<=[" + this.loggerID
					+ "]=> " + "# " + message);
		}
	}

	public void stelog(String message) {
		if (SUPER_TESTER_LOG) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();
			String time = Timestamper.getTimestamp();

			System.err.println("(ThreadID:" + Thread.currentThread().getId() + ") "
					+ "[" + time + " " + className + "." + methodName + "():"
					+ lineNumber + "] " + "<=[" + this.loggerID + "]=> "
					+ "# " + message);
		}
	}

	// dump the stack trace of the current thread
	@SuppressWarnings("static-access")
	public static void dumpStack() {
		Thread.currentThread().dumpStack();
	}
}

