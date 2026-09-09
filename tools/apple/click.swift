import Foundation
import CoreGraphics
guard CommandLine.arguments.count > 2, let x = Double(CommandLine.arguments[1]), let y = Double(CommandLine.arguments[2]) else {
    FileHandle.standardError.write("usage: click <x> <y>\n".data(using: .utf8)!); exit(2) }
let p = CGPoint(x: x, y: y)
CGEvent(mouseEventSource: nil, mouseType: .mouseMoved, mouseCursorPosition: p, mouseButton: .left)?.post(tap: .cghidEventTap)
usleep(120000)
CGEvent(mouseEventSource: nil, mouseType: .leftMouseDown, mouseCursorPosition: p, mouseButton: .left)?.post(tap: .cghidEventTap)
usleep(60000)
CGEvent(mouseEventSource: nil, mouseType: .leftMouseUp, mouseCursorPosition: p, mouseButton: .left)?.post(tap: .cghidEventTap)
