package monifu

import scala.scalajs.test.JasmineTest
import scala.util.Try

object ScalaJSCapabilitiesTest extends JasmineTest {
  describe("Scala.js") {    
    it("should do isInstanceOf checks on traits") {
      val fooObj: Any = ObjFoo
      val barObj: Any = ObjBar

      expect(barObj.isInstanceOf[Foo]).toBe(true)
      expect(fooObj.isInstanceOf[Foo]).toBe(true)
      expect(barObj.isInstanceOf[Bar]).toBe(true)
      expect(fooObj.isInstanceOf[Bar]).toBe(false)

      expect(fooObj.asInstanceOf[Foo].id).toBe("foo")      
      expect(barObj.asInstanceOf[Bar].id).toBe("bar")
      expect(barObj.asInstanceOf[Bar].bar).toBe(true)

      try {
        fooObj.asInstanceOf[Bar].bar
        throw new IllegalStateException("fooObj is not an instance of Bar, so WTF!!!")
      }
      catch {
        case _: ClassCastException =>
          // ignore
      }

      val fooInst: Any = new ClsFoo
      val barInst: Any = new ClsBar

      expect(fooInst.isInstanceOf[Foo]).toBe(true)
      expect(barInst.isInstanceOf[Bar]).toBe(true)
      expect(barInst.isInstanceOf[Foo]).toBe(true)
      expect(fooInst.isInstanceOf[Bar]).toBe(false)

      expect(fooInst.asInstanceOf[Foo].id).toBe("foo")
      expect(barInst.asInstanceOf[Bar].id).toBe("bar")

      expect(barInst.asInstanceOf[Bar].bar).toBe(true)

      try {
        fooInst.asInstanceOf[Bar].bar
        throw new IllegalStateException("fooObj is not an instance of Bar, so WTF!!!")
      }
      catch {
        case _: ClassCastException =>
          // ignore
      }
    }

    it("should do pattern matching involving isInstanceOf and traits") {
      def getName(ref: Any): String = 
        ref match {
          case _: Bar => "bar"
          case _: Foo => "foo"
          case _ => "unknown"
        }

      expect(getName(ObjFoo)).toBe("foo")
      expect(getName(ObjBar)).toBe("bar")
      expect(getName(new ClsFoo())).toBe("foo")
      expect(getName(new ClsBar())).toBe("bar")
    }
  }

  trait Foo {
    def id: String
  }

  trait Bar extends Foo {
    def id: String
    def bar: Boolean
  }

  class ClsBar extends Bar {
    val id = "bar"
    val bar = true
  }

  class ClsFoo extends Foo {
    val id = "foo"
  }

  object ObjBar extends Bar {
    val id = "bar"
    val bar = true
  }

  object ObjFoo extends Foo {
    val id = "foo"
  }
}
