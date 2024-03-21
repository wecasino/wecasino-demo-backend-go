package utils

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const DEFAULT_LOGRUS_LEVEL = log.InfoLevel

func NewLogrus() *log.Logger {
	logger := log.New()
	SetupLogger(logger, nil)
	return logger
}

func SetupStanderLogrus() {
	SetupLogger(log.StandardLogger(), nil)
}

// func InitLog(tagName string) *log.Logger {
// 	SetupLogger(log.StandardLogger(), log.FieldMap{
// 		"App": tagName,
// 	})
// 	return log.StandardLogger()
// }

func SetupLogger(logger *log.Logger, fieldsMap log.FieldMap) {
	if logger == nil {
		return
	}

	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339,
		FieldMap:        fieldsMap,
	})

	_loader := NewEnvLoader(logger)

	// logger.SetReportCaller(_loader.LoadTrue("LOGRUS_REPORT_CALLER"))

	// logrus日志一共7级别, 从高到低: panic, fatal, error, warn, info, debug, trace.
	_level, err := log.ParseLevel(strings.ToLower(_loader.LoadEnvOrDefault("LOGRUS_LEVEL", DEFAULT_LOGRUS_LEVEL.String())))
	if err != nil {
		_level = DEFAULT_LOGRUS_LEVEL
	}
	logger.SetLevel(_level)
	if _level >= log.DebugLevel {
		log.SetReportCaller(true)
	}
	logger.Infof("Use Logger level: %v", _level.String())

}

type LogFormatter struct {
	tag string
}

func InitLog(tagName string) *log.Logger {

	logger := log.New()
	log.SetReportCaller(true)
	log.SetFormatter(&LogFormatter{tag: tagName})
	// log.SetLevel(log.DebugLevel)
	_loader := NewEnvLoader(logger)
	// logrus日志一共7级别, 从高到低: panic, fatal, error, warn, info, debug, trace.
	_level, err := log.ParseLevel(strings.ToLower(_loader.LoadEnvOrDefault("LOGRUS_LEVEL", DEFAULT_LOGRUS_LEVEL.String())))
	if err != nil {
		_level = DEFAULT_LOGRUS_LEVEL
	}
	logger.SetLevel(_level)
	if _level >= log.DebugLevel {
		log.SetReportCaller(true)
	}
	logger.Printf("Use Logger level: %v", _level.String())
	return logger
}

func (s *LogFormatter) Format(entry *log.Entry) ([]byte, error) {
	timestamp := time.Now().Local().Format("2006-01-02 15:04:05")
	// var file string
	var line int
	var funInfo = ""
	var file = ""
	if entry.Caller != nil {
		file = entry.Caller.File
		line = entry.Caller.Line
		funInfo = entry.Caller.Func.Name()
	}
	var levelColor int
	switch entry.Level {
	case log.DebugLevel, log.TraceLevel:
		levelColor = 32 // green
	case log.WarnLevel:
		levelColor = 33 // yellow
	case log.ErrorLevel, log.FatalLevel, log.PanicLevel:
		levelColor = 31 // red
	default:
		levelColor = 36 // blue
	}

	entry.Data["tag"] = s.tag

	var index = strings.LastIndex(funInfo, ".")
	if index != -1 {
		funInfo = funInfo[index+1:]
	}
	// fmt.Println("===========")
	// for key, item := range entry.Data {
	// 	fmt.Printf("key:[%v], item:[%v]\n", key, item)
	// }
	// fmt.Println("===========")

	var msg = ""
	if entry.Data["tag"] != nil {
		if entry.Data["err"] != nil {
			// msg = fmt.Sprintf("[%s]: \x1b[%dm%s [%s:%d] [%s] error:\"%v\"\x1b[0m msg=\"%s\"", timestamp, levelColor, strings.ToUpper(entry.Level.String()), entry.Caller.File, line, entry.Data["tag"], entry.Data["err"], entry.Message)
			msg = fmt.Sprintf("[%s]:[%s] \x1b[%dm[%s][%s][%s:%d] error:\"%v\" \x1b[0mmsg=\"%s\"", timestamp, entry.Data["tag"], levelColor, strings.ToUpper(entry.Level.String()), funInfo, file, line, entry.Data["err"], entry.Message)
		} else if entry.Data[log.ErrorKey] != nil {
			msg = fmt.Sprintf("[%s]:[%s] \x1b[%dm[%s][%s][%s:%d] error:\"%v\" \x1b[0mmsg=\"%s\"", timestamp, entry.Data["tag"], levelColor, strings.ToUpper(entry.Level.String()), funInfo, file, line, entry.Data[log.ErrorKey], entry.Message)
		} else {
			msg = fmt.Sprintf("[%s]:[%s] \x1b[%dm[%s][%s][%s:%d] \x1b[0mmsg=\"%s\"", timestamp, entry.Data["tag"], levelColor, strings.ToUpper(entry.Level.String()), funInfo, file, line, entry.Message)
		}
	} else {
		if entry.Data["err"] != nil {
			msg = fmt.Sprintf("[%s]: \x1b[%dm[%s][%s][%s:%d] error:\"%v\" \x1b[0mmsg=\"%s\"", timestamp, levelColor, strings.ToUpper(entry.Level.String()), funInfo, file, line, entry.Data["err"], entry.Message)
		} else if entry.Data[log.ErrorKey] != nil {
			msg = fmt.Sprintf("[%s]: \x1b[%dm[%s][%s][%s:%d] error:\"%v\" \x1b[0mmsg=\"%s\"", timestamp, levelColor, strings.ToUpper(entry.Level.String()), funInfo, file, line, entry.Data[log.ErrorKey], entry.Message)
		} else {
			msg = fmt.Sprintf("[%s]: \x1b[%dm[%s][%s][%s:%d] \x1b[0mmsg=\"%s\"", timestamp, levelColor, strings.ToUpper(entry.Level.String()), funInfo, file, line, entry.Message)
		}
	}

	var tagInfo = ""
	// span := trace.SpanFromContext(entry.Context)
	// // span.SetAttributes(attribute.Key("testset").String("value"))
	// var checkTraceID = span.SpanContext().TraceID().String()
	// if checkTraceID != "00000000000000000000000000000000" {
	// 	entry.Data["trace_id"] = span.SpanContext().TraceID().String()
	// }
	// var checkSpanID = span.SpanContext().SpanID().String()
	// if checkSpanID != "0000000000000000" {
	// 	entry.Data["span_id"] = span.SpanContext().SpanID().String()
	// }

	for key, item := range entry.Data {
		// fmt.Printf("key:[%v], item:[%v]\n", key, item)
		if key != "tag" && key != "err" && key != log.ErrorKey {
			tagInfo = tagInfo + fmt.Sprintf("[%s]:[%v]", key, item)
		}
	}

	if len(tagInfo) > 0 {
		msg = msg + " fields=>{" + tagInfo + "}\n"
	} else {
		msg = msg + "\n"
	}
	// fmt.Println("\nmsg:" + msg + "\n")

	return []byte(msg), nil
}
