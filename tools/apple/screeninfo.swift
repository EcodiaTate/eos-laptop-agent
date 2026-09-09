import CoreGraphics
import Foundation
let d = CGMainDisplayID()
let ptW = CGDisplayBounds(d).width           // points
let pxW = Double(CGDisplayPixelsWide(d))      // pixels (may equal points on non-retina)
guard let mode = CGDisplayCopyDisplayMode(d) else { print("\(ptW) \(pxW) 1.0"); exit(0) }
let realPxW = Double(mode.pixelWidth)
print("\(ptW) \(realPxW) \(realPxW/ptW)")
