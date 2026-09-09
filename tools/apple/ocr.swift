// ocr.swift - read text from a PNG via Vision (VNRecognizeTextRequest). Prints
// recognised strings one per line. Used to read the 6-digit Apple verification
// code off a screenshot of the trusted-Mac "Get a verification code" sheet.
import Foundation
import Vision
import AppKit

guard CommandLine.arguments.count > 1,
      let img = NSImage(contentsOfFile: CommandLine.arguments[1]),
      let cg = img.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
    FileHandle.standardError.write("usage: ocr.swift <png> (unreadable image)\n".data(using: .utf8)!)
    exit(2)
}
let req = VNRecognizeTextRequest()
req.recognitionLevel = .accurate
req.usesLanguageCorrection = false
let handler = VNImageRequestHandler(cgImage: cg, options: [:])
do {
    try handler.perform([req])
    for obs in (req.results ?? []) {
        if let top = obs.topCandidates(1).first { print(top.string) }
    }
} catch { FileHandle.standardError.write("ocr error: \(error)\n".data(using: .utf8)!); exit(1) }
