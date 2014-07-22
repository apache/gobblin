package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;
import com.jcraft.jsch.UserInfo;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;
import com.linkedin.uif.source.extractor.extract.sftp.SftpCommand.SftpCommandType;

/**
 * Connects to a source via SFTP and
 * executes a given list of SFTP commands
 * @author stakiar
 */
public class SftpExecutor
{   
    private static final String CD = "CD";
    private static final String CHMOD = "CHMOD";
    private static final Set<String> knownCmds = new HashSet<String>(Arrays.asList(CD, CHMOD));
    
    private static Logger log = LoggerFactory.getLogger(SftpExecutor.class);

    /**
     * Opens up a connection to specified host using the username
     * Connects to the source using a private key without prompting
     * for a password. This method does not support connecting to
     * a source using a password, only by private key
     * 
     * @param privateKey is the location of the private key file
     * @param knownHosts is the location of the known hosts file
     * @param userName is the user name to connect with
     * @param hostName is the host name to connect to
     * @return a Session that is connected to the host
     */
    public static Session connect(String privateKey, String knownHosts, String userName,
                                  String hostName, String proxyHost, int proxyPort) {

        JSch.setLogger(new JSchLogger());
        JSch jsch = new JSch();
        Session session = null;
        log.info("Attempting to connect to source via SFTP");
        try {
            jsch.addIdentity(privateKey);
            jsch.setKnownHosts(knownHosts);

            session = jsch.getSession(userName, hostName);
            
            if (proxyHost != null && proxyPort >= 0) {
                session.setProxy(new ProxyHTTP(proxyHost, proxyPort));
            }
            
            UserInfo ui = new MyUserInfo();
            session.setUserInfo(ui);

            // Establish the session connection
            session.connect();

            log.info("Connection established to the SFTP source");
            return session;
        } catch (JSchException e) {
            if (session != null) {
                session.disconnect();
            }
            log.error("Failed to establish a connection to the SFTP source", e);
            throw new RuntimeException("Failed to establish a connection to the SFTP source", e);
        }
    }

    /**
     * Given a semicolon separate list of shell commands,
     * this method converts the command types and arguments
     * into SftpCommands
     * 
     * @param input string to convert to SftpCommands
     * @return a list of SftpCommands
     */
    public static List<Command> parseInputCommands(String input) {        
        List<Command> cmds = new ArrayList<Command>();
        Iterable<String> inputList = Splitter.on(";").trimResults().split(input);
        
        for (String cmd : inputList) {
            for (String knownCmd : knownCmds) {
                if (cmd.toUpperCase().startsWith(knownCmd)) {
                    String[] cmdParams = cmd.substring(knownCmd.length() + 1).split("\\s+");
                    cmds.add(new SftpCommand().build(Arrays.asList(cmdParams), SftpCommandType.valueOf(knownCmd)));
                }
            }
        }
        return cmds;
    }
    
    /**
     * Takes in a list of SftpCommands to execute
     * 
     * @param cmds is the list of commands to execute
     * @param sftp is the channel to execute the commands on
     * @return a list of CommandOutputs
     * @throws SftpException
     * @throws SftpCommandFormatException
     */
    public static CommandOutput<SftpCommand, List<String>> executeUnixCommands(List<Command> cmds, ChannelSftp sftp) throws SftpException, SftpCommandFormatException {
        CommandOutput<SftpCommand, List<String>> output = new SftpCommandOutput();
        for (Command cmd : cmds) {
            if (cmd instanceof SftpCommand) {
                SftpCommand sftpCmd = (SftpCommand) cmd;
                output.put(sftpCmd, SftpExecutor.executeUnixCommand(sftpCmd, sftp));
            }
        }
        return output;
    }
    
    /**
     * Executes a specified SftpUnixCommand
     * This method only supports basic shell
     * commands such as cd, ls, etc.
     * It does not support the GET command
     * 
     * @param cmd is the command to execute
     * @param sftp is the channel to execute the command on
     * @return a list representing the output of the command
     * @throws SftpCommandFormatException
     * @throws SftpException
     */
    public static List<String> executeUnixCommand(SftpCommand cmd, ChannelSftp sftp) throws SftpCommandFormatException, SftpException {
        List<String> results = new ArrayList<String>();
        List<String> params = cmd.getParams();
        SftpCommandType type = (SftpCommandType) cmd.getCommandType();
        
        if (sftp == null) {
            throw new SftpCommandFormatException("SFTP instance not initialized");
        }

        switch(type) {
            case CD:
                if (params.size() != 1) {
                    throw new SftpCommandFormatException("CD command must have one argument");
                }
                sftp.cd(params.get(0));
                break;
            case CHMOD:
                if (params.size() == 2) {
                    throw new SftpCommandFormatException("CHMOD command must have two arguments");
                }
                try {
                    sftp.chmod(Integer.parseInt(params.get(0)), params.get(1));
                } catch (NumberFormatException e) {
                    throw new SftpCommandFormatException("CHMOD command passed a non-int value", e);
                }
                break;
            case LS:
                Vector<LsEntry> lsOut;
                if (params.size() == 0) {
                    lsOut = (Vector<LsEntry>) sftp.ls("");
                } else if (params.size() == 1) {
                    lsOut = (Vector<LsEntry>) sftp.ls(params.get(0));
                } else {
                    throw new SftpCommandFormatException("LS command can only have max 1 argument");
                }
                for (LsEntry entry : lsOut) {
                    results.add(entry.getFilename());
                }
                break;
            case GET_FILE:
                log.error("Command get file cannnot be executed in this method as it is not a unix command");
                break;
            case GET_STREAM:
                log.error("Command get stream cannnot be executed in this method as it is not a unix command");
                break;
            default:
                log.error("CommandType " + type.toString() + " not recognized");
                break;
        }
        return results;
    }
    
    /**
     * Executes a get SftpCommand and downloads
     * the file to a specified location
     * @param cmd is the command to execute
     * @param sftp is the channel to execute the command on
     * @throws SftpException
     * @throws SftpCommandFormatException
     * @throws IOException 
     */
    public void executeGetFileCommand(SftpCommand cmd, ChannelSftp sftp, String destUri) throws SftpException, SftpCommandFormatException, IOException {
        if (!cmd.getCommandType().equals(SftpCommandType.GET_FILE)) {
            throw new SftpCommandFormatException("Command must be of type GET_FILE");
        }
        List<String> params = cmd.getParams();
        SftpGetMonitor monitor = new SftpGetMonitor();
        if (params.size() == 2) {
            log.info("Attempting to download file: " + params.get(0) + " to dest: " + params.get(1));
            FileOutputStream out = null;
            try {
                out = new FileOutputStream(new File(params.get(1)));
                sftp.get(params.get(0), out, monitor);
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else {
            throw new SftpCommandFormatException("GET command does correct number of arguments");
        }
    }
    
    /**
     * Executes a get SftpCommand and returns
     * an input stream to the file
     * @param cmd is the command to execute
     * @param sftp is the channel to execute the command on
     * @throws SftpException
     * @throws SftpCommandFormatException
     */
    public InputStream executeGetStreamCommand(SftpCommand cmd, ChannelSftp sftp) throws SftpException, SftpCommandFormatException {
        if (!cmd.getCommandType().equals(SftpCommandType.GET_STREAM)) {
            throw new SftpCommandFormatException("Command must be of type GET_STREAM");
        }
        
        List<String> params = cmd.getParams();
        SftpGetMonitor monitor = new SftpGetMonitor();
        InputStream stream = null;
        if (params.size() == 1) {
            stream = sftp.get(params.get(0), monitor);
        } else if (params.size() == 2) {
            try{
                stream = sftp.get(params.get(0), monitor, Long.parseLong(params.get(1)));
            } catch (NumberFormatException e) {
                throw new SftpCommandFormatException("GET command passed a non-long value", e);
            }
        } else {
            throw new SftpCommandFormatException("GET command does correct number of arguments");
        }
        return stream;
    }
    
    /**
     * Implementation of an SftpProgressMonitor
     * to monitor the progress of file downloads
     * using the ChannelSftp.GET methods
     * @author stakiar
     */
    private class SftpGetMonitor implements SftpProgressMonitor {

        private int op;
        private String src;
        private String dest;
        private long totalCount;
        
        @Override
        public void init(int op, String src, String dest, long max)
        {
            this.op = op;
            this.src = src;
            this.dest = dest;
            log.info("Operation GET (" + op + ") has started with src: " + src + " dest: " + dest + " and file length: " + max);
        }

        @Override
        public boolean count(long count)
        {
            this.totalCount += count;
            log.info("Transfer is in progress for file: " + src + ". Finished transferring " + this.totalCount + " bytes");
            return true;
        }

        @Override
        public void end()
        {
            log.info("Data transfer has finished for operation " + this.op + " src: " + this.src + " dest: " + this.dest);
        }
    }
    
    /**
     * Basic implementation of jsch.Logger
     * that logs the output from the JSch
     * commands to slf4j
     * @author stakiar
     */
    public static class JSchLogger implements com.jcraft.jsch.Logger {
        public boolean isEnabled(int level) {
            switch (level) {
            case DEBUG:
                return log.isDebugEnabled();
            case INFO:
                return log.isInfoEnabled();
            case WARN:
                return log.isWarnEnabled();
            case ERROR:
                return log.isErrorEnabled();
            case FATAL:
                return log.isErrorEnabled();
            }
            return false;
        }

        public void log(int level, String message) {
            switch (level) {
            case DEBUG:
                log.debug(message);
                break;
            case INFO:
                log.info(message);
                break;
            case WARN:
                log.warn(message);
                break;
            case ERROR:
                log.error(message);
                break;
            case FATAL:
                log.error(message);
                break;
            }
        }
    }
    
    /**
     * Implementation of UserInfo class
     * for JSch which allows for password-less
     * login via keys
     * @author stakiar
     */
    public static class MyUserInfo implements UserInfo {

        // The passphrase used to access the private key
        @Override
        public String getPassphrase()
        {
            return null;
        }

        // The password to login to the client server
        @Override
        public String getPassword()
        {
            return null;
        }

        @Override
        public boolean promptPassword(String message)
        {
            return true;
        }

        @Override
        public boolean promptPassphrase(String message)
        {
            return true;
        }

        @Override
        public boolean promptYesNo(String message)
        {
            return true;
        }

        @Override
        public void showMessage(String message)
        {
            log.info(message);
        }
    }
}