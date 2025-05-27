import { useRef, useEffect } from 'react';
import p5 from 'p5';

export default function WaveSketch() {
  const containerRef = useRef();

  useEffect(() => {
    let myP5;

    const sketch = (p) => {
      let t = 0;

      p.setup = () => {
        const canvas = p.createCanvas(p.windowWidth, 180);
        canvas.parent(containerRef.current);
        p.noFill();
      };

      p.draw = () => {
        p.clear(); 
        
        const waveColor = p.color(255, 215, 0, 180);
        p.stroke(waveColor);
        
        drawWave(p, 0.0015, 40, 100, 1.8, t);
        drawWave(p, 0.002, 30, 70, 1.1, t * 1.3);
        drawWave(p, 0.001, 50, 130, 0.8, t * 0.8);
        
        t += 0.01;
      };
      const drawWave = (p, frequency, amplitude, yPos, thickness, time) => {
        p.strokeWeight(thickness);
        p.beginShape();
        for (let x = 0; x < p.width; x++) {
          
          const y = yPos + amplitude * p.sin(frequency * x * p.TWO_PI + time);
          p.vertex(x, y);
        }
        p.endShape();
      };

      p.windowResized = () => {
        p.resizeCanvas(p.windowWidth, 180);
      };
    };

    myP5 = new p5(sketch);

    return () => myP5.remove();
  }, []);

  return <div ref={containerRef} style={{ position: 'fixed', top: 0, left: 0, width: '100%', zIndex: 0 }} />;
}