import Foundation
import Vision
import AppKit
guard CommandLine.arguments.count > 1,
      let img = NSImage(contentsOfFile: CommandLine.arguments[1]),
      let cg = img.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
    FileHandle.standardError.write("usage: ocrbox <png>\n".data(using: .utf8)!); exit(2) }
let W = Double(cg.width), H = Double(cg.height)
let req = VNRecognizeTextRequest(); req.recognitionLevel = .accurate; req.usesLanguageCorrection = false
try VNImageRequestHandler(cgImage: cg, options: [:]).perform([req])
for obs in (req.results ?? []) {
    guard let t = obs.topCandidates(1).first else { continue }
    let b = obs.boundingBox // normalized, origin BOTTOM-left
    let cx = (b.minX + b.width/2) * W
    let cy = (1.0 - (b.minY + b.height/2)) * H // flip to top-left origin
    print("\(t.string)\t\(Int(cx))\t\(Int(cy))\t\(Int(W))\t\(Int(H))")
}
